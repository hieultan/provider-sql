package publication

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

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
	errUpdatePublication = "cannot update publication"
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
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrency}).
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
		Owner:     new(string),
		AllTables: new(bool),
		Publish:   []string{},
		Tables:    []string{},
	}

	query := "SELECT r.rolname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate " +
		"FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_roles r ON p.pubowner = r.oid WHERE p.pubname = $1"

	var ins, upd, del, trunc bool
	err := c.db.Scan(ctx, xsql.Query{String: query, Parameters: []interface{}{meta.GetExternalName(cr)}},
		observed.Owner, observed.AllTables, &ins, &upd, &del, &trunc)
	if xsql.IsNoRows(err) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
	}

	if ins {
		observed.Publish = append(observed.Publish, "insert")
	}
	if upd {
		observed.Publish = append(observed.Publish, "update")
	}
	if del {
		observed.Publish = append(observed.Publish, "delete")
	}
	if trunc {
		observed.Publish = append(observed.Publish, "truncate")
	}

	rows, err := c.db.Query(ctx, xsql.Query{String: `SELECT CONCAT(schemaname,'.',tablename) FROM pg_catalog.pg_publication_tables WHERE pubname = $1`, Parameters: []interface{}{meta.GetExternalName(cr)}})
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var tbl string
			if err := rows.Scan(&tbl); err != nil {
				return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
			}
			observed.Tables = append(observed.Tables, tbl)
		}
		if err := rows.Err(); err != nil {
			return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
		}
	}

	cr.SetConditions(xpv1.Available())
	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, cr.Spec.ForProvider),
	}, nil
}

func tablesClause(p v1alpha1.PublicationParameters) string {
	if p.AllTables != nil && *p.AllTables {
		return "FOR ALL TABLES"
	}
	if len(p.Tables) > 0 {
		quoted := make([]string, len(p.Tables))
		for i, t := range p.Tables {
			quoted[i] = t // assume already schema.table format
		}
		return fmt.Sprintf("FOR TABLE %s", strings.Join(quoted, ", "))
	}
	return ""
}

func paramsClause(p v1alpha1.PublicationParameters, create bool) string {
	var params []string
	if len(p.Publish) > 0 {
		params = append(params, fmt.Sprintf("publish = '%s'", strings.Join(p.Publish, ", ")))
	}
	if p.PublishViaPartitionRoot != nil {
		params = append(params, fmt.Sprintf("publish_via_partition_root = %t", *p.PublishViaPartitionRoot))
	}
	if len(params) == 0 {
		return ""
	}
	if create {
		return fmt.Sprintf("WITH (%s)", strings.Join(params, ", "))
	}
	return fmt.Sprintf("SET (%s)", strings.Join(params, ", "))
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotPublication)
	}

	name := pq.QuoteIdentifier(meta.GetExternalName(cr))
	var b strings.Builder
	b.WriteString("CREATE PUBLICATION ")
	b.WriteString(name)
	if tc := tablesClause(cr.Spec.ForProvider); tc != "" {
		b.WriteString(" ")
		b.WriteString(tc)
	}
	if pc := paramsClause(cr.Spec.ForProvider, true); pc != "" {
		b.WriteString(" ")
		b.WriteString(pc)
	}

	if err := c.db.Exec(ctx, xsql.Query{String: b.String()}); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreatePublication)
	}

	if cr.Spec.ForProvider.Owner != nil {
		q := fmt.Sprintf("ALTER PUBLICATION %s OWNER TO %s", name, pq.QuoteIdentifier(*cr.Spec.ForProvider.Owner))
		if err := c.db.Exec(ctx, xsql.Query{String: q}); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, errCreatePublication)
		}
	}

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotPublication)
	}

	name := pq.QuoteIdentifier(meta.GetExternalName(cr))
	if cr.Spec.ForProvider.Owner != nil {
		q := fmt.Sprintf("ALTER PUBLICATION %s OWNER TO %s", name, pq.QuoteIdentifier(*cr.Spec.ForProvider.Owner))
		if err := c.db.Exec(ctx, xsql.Query{String: q}); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errUpdatePublication)
		}
	}

	if len(cr.Spec.ForProvider.Tables) > 0 || cr.Spec.ForProvider.AllTables != nil {
		// For simplicity drop all current tables and re-add
		if err := c.db.Exec(ctx, xsql.Query{String: fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", name, tablesClause(cr.Spec.ForProvider))}); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errUpdatePublication)
		}
	}

	if pc := paramsClause(cr.Spec.ForProvider, false); pc != "" {
		q := fmt.Sprintf("ALTER PUBLICATION %s %s", name, pc)
		if err := c.db.Exec(ctx, xsql.Query{String: q}); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errUpdatePublication)
		}
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return errors.New(errNotPublication)
	}

	name := pq.QuoteIdentifier(meta.GetExternalName(cr))
	mode := "RESTRICT"
	if cr.Spec.ForProvider.DropCascade != nil && *cr.Spec.ForProvider.DropCascade {
		mode = "CASCADE"
	}
	q := fmt.Sprintf("DROP PUBLICATION %s %s", name, mode)
	return errors.Wrap(c.db.Exec(ctx, xsql.Query{String: q}), errDropPublication)
}

func upToDate(observed, desired v1alpha1.PublicationParameters) bool {
	return cmp.Equal(observed, desired,
		cmpopts.IgnoreFields(v1alpha1.PublicationParameters{}, "Database", "DatabaseRef", "DatabaseSelector", "OwnerRef", "OwnerSelector", "DropCascade"),
		cmpopts.EquateEmpty())
}

func lateInit(obs v1alpha1.PublicationParameters, p *v1alpha1.PublicationParameters) bool {
	li := false
	if p.Owner == nil && obs.Owner != nil {
		p.Owner = obs.Owner
		li = true
	}
	if p.AllTables == nil && obs.AllTables != nil {
		p.AllTables = obs.AllTables
		li = true
	}
	if len(p.Tables) == 0 && len(obs.Tables) > 0 {
		p.Tables = obs.Tables
		li = true
	}
	if len(p.Publish) == 0 && len(obs.Publish) > 0 {
		p.Publish = obs.Publish
		li = true
	}
	return li
}
