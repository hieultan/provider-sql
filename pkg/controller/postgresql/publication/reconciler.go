package publication

import (
	"context"
	"fmt"
	"sort"
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
	errAlterPublication  = "cannot alter publication"
	errDropPublication   = "cannot drop publication"
	errNoDatabase        = "database must be specified"

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

	if cr.Spec.ForProvider.Database == nil {
		return nil, errors.New(errNoDatabase)
	}

	return &external{db: c.newDB(s.Data, *cr.Spec.ForProvider.Database, clients.ToString(pc.Spec.SSLMode))}, nil
}

type external struct{ db xsql.DB }

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) { //nolint:gocyclo
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotPublication)
	}

	observed := v1alpha1.PublicationParameters{
		Owner:                   new(string),
		AllTables:               new(bool),
		PublishViaPartitionRoot: new(bool),
	}
	var pubInsert, pubUpdate, pubDelete, pubTruncate bool

	q := "SELECT r.rolname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot FROM pg_catalog.pg_publication p JOIN pg_catalog.pg_roles r ON p.pubowner = r.oid WHERE p.pubname = $1"
	err := c.db.Scan(ctx, xsql.Query{String: q, Parameters: []interface{}{meta.GetExternalName(cr)}},
		observed.Owner,
		observed.AllTables,
		&pubInsert,
		&pubUpdate,
		&pubDelete,
		&pubTruncate,
		observed.PublishViaPartitionRoot,
	)

	if xsql.IsNoRows(err) || postgresql.IsInvalidCatalog(err) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
	}

	if pubInsert {
		observed.Publish = append(observed.Publish, "insert")
	}
	if pubUpdate {
		observed.Publish = append(observed.Publish, "update")
	}
	if pubDelete {
		observed.Publish = append(observed.Publish, "delete")
	}
	if pubTruncate {
		observed.Publish = append(observed.Publish, "truncate")
	}

	rows, err := c.db.Query(ctx, xsql.Query{String: "SELECT CONCAT(schemaname,'.',tablename) FROM pg_catalog.pg_publication_tables WHERE pubname = $1", Parameters: []interface{}{meta.GetExternalName(cr)}})
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
	}
	defer rows.Close()

	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
		}
		observed.Tables = append(observed.Tables, t)
	}
	if err := rows.Err(); err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectPublication)
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, cr.Spec.ForProvider),
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotPublication)
	}

	var ql []xsql.Query
	cr.SetConditions(xpv1.Creating())
	createPublicationQueries(cr.Spec.ForProvider, &ql, meta.GetExternalName(cr))
	err := c.db.ExecTx(ctx, ql)
	return managed.ExternalCreation{}, errors.Wrap(err, errCreatePublication)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotPublication)
	}
	var ql []xsql.Query
	updatePublicationQueries(cr.Spec.ForProvider, &ql, meta.GetExternalName(cr))
	err := c.db.ExecTx(ctx, ql)
	return managed.ExternalUpdate{}, errors.Wrap(err, errAlterPublication)
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Publication)
	if !ok {
		return errors.New(errNotPublication)
	}
	mode := "RESTRICT"
	if cr.Spec.ForProvider.DropCascade != nil && *cr.Spec.ForProvider.DropCascade {
		mode = "CASCADE"
	}
	q := fmt.Sprintf("DROP PUBLICATION %s %s", pq.QuoteIdentifier(meta.GetExternalName(cr)), mode)
	err := c.db.Exec(ctx, xsql.Query{String: q})
	return errors.Wrap(err, errDropPublication)
}

func upToDate(obs, desired v1alpha1.PublicationParameters) bool {
	if desired.Owner != nil && (obs.Owner == nil || *desired.Owner != *obs.Owner) {
		return false
	}
	if desired.AllTables != nil && (obs.AllTables == nil || *desired.AllTables != *obs.AllTables) {
		return false
	}
	if !equalStringSlices(obs.Tables, desired.Tables) {
		return false
	}
	if !equalStringSlices(opsToStringSlice(obs.Publish), opsToStringSlice(desired.Publish)) {
		return false
	}
	if desired.PublishViaPartitionRoot != nil && (obs.PublishViaPartitionRoot == nil || *desired.PublishViaPartitionRoot != *obs.PublishViaPartitionRoot) {
		return false
	}
	return true
}

func lateInit(obs v1alpha1.PublicationParameters, desired *v1alpha1.PublicationParameters) bool {
	li := false
	if desired.Owner == nil && obs.Owner != nil {
		desired.Owner = obs.Owner
		li = true
	}
	if desired.AllTables == nil && obs.AllTables != nil {
		desired.AllTables = obs.AllTables
		li = true
	}
	if len(desired.Tables) == 0 && len(obs.Tables) > 0 {
		desired.Tables = obs.Tables
		li = true
	}
	if len(desired.Publish) == 0 && len(obs.Publish) > 0 {
		desired.Publish = obs.Publish
		li = true
	}
	if desired.PublishViaPartitionRoot == nil && obs.PublishViaPartitionRoot != nil {
		desired.PublishViaPartitionRoot = obs.PublishViaPartitionRoot
		li = true
	}
	return li
}

func createPublicationQueries(sp v1alpha1.PublicationParameters, ql *[]xsql.Query, name string) { //nolint:gocyclo
	var b strings.Builder
	b.WriteString("CREATE PUBLICATION ")
	b.WriteString(pq.QuoteIdentifier(name))

	if sp.AllTables != nil && *sp.AllTables {
		b.WriteString(" FOR ALL TABLES")
	} else if len(sp.Tables) > 0 {
		b.WriteString(" FOR TABLE ")
		for i, t := range sp.Tables {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteTableName(t))
		}
	}

	if params := publicationParameters(sp); params != "" {
		b.WriteString(" WITH (")
		b.WriteString(params)
		b.WriteString(")")
	}

	*ql = append(*ql, xsql.Query{String: b.String()})

	if sp.Owner != nil {
		*ql = append(*ql, xsql.Query{String: fmt.Sprintf("ALTER PUBLICATION %s OWNER TO %s", pq.QuoteIdentifier(name), pq.QuoteIdentifier(*sp.Owner))})
	}
}

func updatePublicationQueries(sp v1alpha1.PublicationParameters, ql *[]xsql.Query, name string) { //nolint:gocyclo
	if sp.Owner != nil {
		*ql = append(*ql, xsql.Query{String: fmt.Sprintf("ALTER PUBLICATION %s OWNER TO %s", pq.QuoteIdentifier(name), pq.QuoteIdentifier(*sp.Owner))})
	}

	if sp.AllTables != nil && *sp.AllTables {
		*ql = append(*ql, xsql.Query{String: fmt.Sprintf("ALTER PUBLICATION %s SET TABLE ALL TABLES", pq.QuoteIdentifier(name))})
	} else if len(sp.Tables) > 0 {
		var b strings.Builder
		b.WriteString("ALTER PUBLICATION ")
		b.WriteString(pq.QuoteIdentifier(name))
		b.WriteString(" SET TABLE ")
		for i, t := range sp.Tables {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteTableName(t))
		}
		*ql = append(*ql, xsql.Query{String: b.String()})
	}

	if params := publicationParameters(sp); params != "" {
		q := fmt.Sprintf("ALTER PUBLICATION %s SET (%s)", pq.QuoteIdentifier(name), params)
		*ql = append(*ql, xsql.Query{String: q})
	}
}

func publicationParameters(sp v1alpha1.PublicationParameters) string {
	var params []string
	if len(sp.Publish) > 0 {
		params = append(params, fmt.Sprintf("publish = '%s'", strings.Join(opsToStringSlice(sp.Publish), ", ")))
	}
	if sp.PublishViaPartitionRoot != nil {
		params = append(params, fmt.Sprintf("publish_via_partition_root = %v", *sp.PublishViaPartitionRoot))
	}
	return strings.Join(params, ", ")
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aCopy := append([]string(nil), a...)
	bCopy := append([]string(nil), b...)
	sort.Strings(aCopy)
	sort.Strings(bCopy)
	for i := range aCopy {
		if aCopy[i] != bCopy[i] {
			return false
		}
	}
	return true
}

func opsToStringSlice(in []v1alpha1.PublicationOperation) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func quoteTableName(t string) string {
	parts := strings.Split(t, ".")
	if len(parts) == 2 {
		return pq.QuoteIdentifier(parts[0]) + "." + pq.QuoteIdentifier(parts[1])
	}
	return pq.QuoteIdentifier(t)
}
