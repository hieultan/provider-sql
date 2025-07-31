/*
Copyright 2024 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package publication

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/feature"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	"github.com/crossplane-contrib/provider-sql/apis/postgresql/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/postgresql"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errNoSecretRef  = "ProviderConfig does not reference a credentials Secret"
	errGetSecret    = "cannot get credentials Secret"

	errNotPublication    = "managed resource is not a Publication custom resource"
	errSelectPublication = "cannot select publication"
	errCreatePublication = "cannot create publication"
	errDropPublication   = "cannot drop publication"

	maxConcurrency = 5
)

// Setup adds a controller that reconciles Publication managed resources.
func Setup(mgr ctrl.Manager, o xpcontroller.Options) error {
	name := managed.ControllerName(v1alpha1.PublicationGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	reconcilerOptions := []managed.ReconcilerOption{
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newDB: postgresql.New}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
	}
	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		reconcilerOptions = append(reconcilerOptions, managed.WithManagementPolicies())
	}
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.PublicationGroupVersionKind),
		reconcilerOptions...,
	)
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Publication{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrency,
		}).
		Complete(r)
}

type connector struct {
	kube  client.Client
	usage resource.Tracker
	newDB func(creds map[string][]byte, database string, sslmode string) xsql.DB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return nil, errors.New(errNotPublication)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &v1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	ref := pc.Spec.Credentials.ConnectionSecretRef
	if ref == nil {
		return nil, errors.New(errNoSecretRef)
	}

	s := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, s); err != nil {
		return nil, errors.Wrap(err, errGetSecret)
	}

	dbName := pc.Spec.DefaultDatabase
	if cr.Spec.ForProvider.Database != nil {
		dbName = *cr.Spec.ForProvider.Database
	}

	return &external{db: c.newDB(s.Data, dbName, clients.ToString(pc.Spec.SSLMode))}, nil
}

type external struct{ db xsql.DB }

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotPublication)
	}

	observed := v1alpha1.PublicationParameters{
		AllTables: new(bool),
		Publish: &v1alpha1.PublicationPublish{
			Insert:   new(bool),
			Update:   new(bool),
			Delete:   new(bool),
			Truncate: new(bool),
		},
		PublishViaPartitionRoot: new(bool),
	}

	query := "SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_publication WHERE pubname=$1"
	err := c.db.Scan(ctx, xsql.Query{String: query, Parameters: []interface{}{meta.GetExternalName(cr)}},
		observed.AllTables,
		observed.Publish.Insert,
		observed.Publish.Update,
		observed.Publish.Delete,
		observed.Publish.Truncate,
		observed.PublishViaPartitionRoot,
	)
	if xsql.IsNoRows(err) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, cr.Spec.ForProvider),
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) { //nolint:gocyclo
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotPublication)
	}

	var b strings.Builder
	b.WriteString("CREATE PUBLICATION ")
	b.WriteString(pq.QuoteIdentifier(meta.GetExternalName(cr)))

	if cr.Spec.ForProvider.AllTables != nil && *cr.Spec.ForProvider.AllTables {
		b.WriteString(" FOR ALL TABLES")
	} else if len(cr.Spec.ForProvider.Tables) > 0 {
		b.WriteString(" FOR TABLE ")
		for i, t := range cr.Spec.ForProvider.Tables {
			if i != 0 {
				b.WriteString(", ")
			}
			b.WriteString(t)
		}
	}

	var opts []string
	if p := cr.Spec.ForProvider.Publish; p != nil {
		var ops []string
		if p.Insert != nil && *p.Insert {
			ops = append(ops, "insert")
		}
		if p.Update != nil && *p.Update {
			ops = append(ops, "update")
		}
		if p.Delete != nil && *p.Delete {
			ops = append(ops, "delete")
		}
		if p.Truncate != nil && *p.Truncate {
			ops = append(ops, "truncate")
		}
		if len(ops) > 0 {
			opts = append(opts, fmt.Sprintf("publish = '%s'", strings.Join(ops, ", ")))
		}
	}
	if cr.Spec.ForProvider.PublishViaPartitionRoot != nil {
		opts = append(opts, fmt.Sprintf("publish_via_partition_root = %t", *cr.Spec.ForProvider.PublishViaPartitionRoot))
	}
	if len(opts) > 0 {
		b.WriteString(" WITH (")
		b.WriteString(strings.Join(opts, ", "))
		b.WriteString(")")
	}

	return managed.ExternalCreation{}, errors.Wrap(c.db.Exec(ctx, xsql.Query{String: b.String()}), errCreatePublication)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	_, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotPublication)
	}
	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return errors.New(errNotPublication)
	}
	err := c.db.Exec(ctx, xsql.Query{String: "DROP PUBLICATION IF EXISTS " + pq.QuoteIdentifier(meta.GetExternalName(cr))})
	return errors.Wrap(err, errDropPublication)
}

func upToDate(observed, desired v1alpha1.PublicationParameters) bool {
	if desired.AllTables != nil && observed.AllTables != nil && *desired.AllTables != *observed.AllTables {
		return false
	}
	if desired.PublishViaPartitionRoot != nil && observed.PublishViaPartitionRoot != nil && *desired.PublishViaPartitionRoot != *observed.PublishViaPartitionRoot {
		return false
	}
	if desired.Publish != nil && observed.Publish != nil {
		if (desired.Publish.Insert != nil && observed.Publish.Insert != nil && *desired.Publish.Insert != *observed.Publish.Insert) ||
			(desired.Publish.Update != nil && observed.Publish.Update != nil && *desired.Publish.Update != *observed.Publish.Update) ||
			(desired.Publish.Delete != nil && observed.Publish.Delete != nil && *desired.Publish.Delete != *observed.Publish.Delete) ||
			(desired.Publish.Truncate != nil && observed.Publish.Truncate != nil && *desired.Publish.Truncate != *observed.Publish.Truncate) {
			return false
		}
	}
	return true
}

func lateInit(observed v1alpha1.PublicationParameters, desired *v1alpha1.PublicationParameters) bool {
	li := false
	if desired.AllTables == nil && observed.AllTables != nil {
		desired.AllTables = observed.AllTables
		li = true
	}
	if desired.PublishViaPartitionRoot == nil && observed.PublishViaPartitionRoot != nil {
		desired.PublishViaPartitionRoot = observed.PublishViaPartitionRoot
		li = true
	}
	if desired.Publish == nil && observed.Publish != nil {
		desired.Publish = observed.Publish
		li = true
	} else if desired.Publish != nil && observed.Publish != nil {
		if desired.Publish.Insert == nil {
			desired.Publish.Insert = observed.Publish.Insert
			li = true
		}
		if desired.Publish.Update == nil {
			desired.Publish.Update = observed.Publish.Update
			li = true
		}
		if desired.Publish.Delete == nil {
			desired.Publish.Delete = observed.Publish.Delete
			li = true
		}
		if desired.Publish.Truncate == nil {
			desired.Publish.Truncate = observed.Publish.Truncate
			li = true
		}
	}
	return li
}
