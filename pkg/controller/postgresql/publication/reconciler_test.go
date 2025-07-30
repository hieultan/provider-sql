package publication

import (
	"context"
	"database/sql"
	"testing"

	"github.com/crossplane-contrib/provider-sql/apis/postgresql/v1alpha1"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/test"

	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
)

type mockDB struct {
	MockExec                 func(ctx context.Context, q xsql.Query) error
	MockExecTx               func(ctx context.Context, ql []xsql.Query) error
	MockScan                 func(ctx context.Context, q xsql.Query, dest ...interface{}) error
	MockGetConnectionDetails func(username, password string) managed.ConnectionDetails
}

func (m mockDB) Exec(ctx context.Context, q xsql.Query) error      { return m.MockExec(ctx, q) }
func (m mockDB) ExecTx(ctx context.Context, ql []xsql.Query) error { return m.MockExecTx(ctx, ql) }
func (m mockDB) Scan(ctx context.Context, q xsql.Query, dest ...interface{}) error {
	return m.MockScan(ctx, q, dest...)
}
func (m mockDB) Query(ctx context.Context, q xsql.Query) (*sql.Rows, error) { return &sql.Rows{}, nil }
func (m mockDB) GetConnectionDetails(username, password string) managed.ConnectionDetails {
	return m.MockGetConnectionDetails(username, password)
}

func TestConnect(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		kube  client.Client
		usage resource.Tracker
		newDB func(creds map[string][]byte, database string, sslmode string) xsql.DB
	}
	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   error
	}{
		"ErrNotPublication": {
			reason: "An error should be returned if the managed resource is not a Publication",
			args:   args{mg: nil},
			want:   errors.New(errNotPublication),
		},
		"ErrTrackProviderConfigUsage": {
			reason: "An error should be returned if we can't track ProviderConfig usage",
			fields: fields{usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return errBoom })},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   errors.Wrap(errBoom, errTrackPCUsage),
		},
		"ErrGetProviderConfig": {
			reason: "An error should be returned if we can't get ProviderConfig",
			fields: fields{
				kube:  &test.MockClient{MockGet: test.NewMockGetFn(errBoom)},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
			},
			args: args{mg: &v1alpha1.Publication{Spec: v1alpha1.PublicationSpec{ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{}}}}},
			want: errors.Wrap(errBoom, errGetPC),
		},
		"ErrMissingConnectionSecret": {
			reason: "An error should be returned if ProviderConfig lacks secret",
			fields: fields{
				kube:  &test.MockClient{MockGet: test.NewMockGetFn(nil)},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
			},
			args: args{mg: &v1alpha1.Publication{Spec: v1alpha1.PublicationSpec{ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{}}}}},
			want: errors.New(errNoSecretRef),
		},
		"ErrGetSecret": {
			reason: "An error should be returned if we can't get secret",
			fields: fields{
				kube: &test.MockClient{MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
					switch o := obj.(type) {
					case *v1alpha1.ProviderConfig:
						o.Spec.Credentials.ConnectionSecretRef = &xpv1.SecretReference{}
					case *corev1.Secret:
						return errBoom
					}
					return nil
				})},
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return nil }),
			},
			args: args{mg: &v1alpha1.Publication{Spec: v1alpha1.PublicationSpec{ResourceSpec: xpv1.ResourceSpec{ProviderConfigReference: &xpv1.Reference{}}}}},
			want: errors.Wrap(errBoom, errGetSecret),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := &connector{kube: tc.fields.kube, usage: tc.fields.usage, newDB: tc.fields.newDB}
			_, err := e.Connect(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want, err, test.EquateErrors()); diff != "" {
				t.Errorf("%s\nconnect: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	errBoom := errors.New("boom")
	type fields struct{ db xsql.DB }
	type args struct {
		ctx context.Context
		mg  resource.Managed
	}
	type want struct{ err error }

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotPublication": {
			reason: "An error should be returned if the managed resource is not a Publication",
			args:   args{mg: nil},
			want:   want{err: errors.New(errNotPublication)},
		},
		"ErrExec": {
			reason: "Errors executing create should be returned",
			fields: fields{db: &mockDB{MockExec: func(ctx context.Context, q xsql.Query) error { return errBoom }}},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   want{err: errors.Wrap(errBoom, errCreatePublication)},
		},
		"Success": {
			reason: "No error on successful create",
			fields: fields{db: &mockDB{MockExec: func(ctx context.Context, q xsql.Query) error { return nil }}},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   want{err: nil},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			_, err := e.Create(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("%s\ncreate: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestObserve(t *testing.T) {
	errBoom := errors.New("boom")
	type fields struct{ db xsql.DB }
	type args struct {
		ctx context.Context
		mg  resource.Managed
	}
	type want struct {
		o   managed.ExternalObservation
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotPublication": {
			reason: "Error if managed resource is not Publication",
			args:   args{mg: nil},
			want:   want{err: errors.New(errNotPublication)},
		},
		"ErrNoPublication": {
			reason: "ResourceExists false when not found",
			fields: fields{db: &mockDB{MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error { return sql.ErrNoRows }}},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   want{o: managed.ExternalObservation{ResourceExists: false}},
		},
		"ErrSelectPublication": {
			reason: "Return errors selecting publication",
			fields: fields{db: &mockDB{MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error { return errBoom }}},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   want{err: errors.Wrap(errBoom, errSelectPublication)},
		},
		"Success": {
			reason: "No error on successful observe",
			fields: fields{db: &mockDB{MockScan: func(ctx context.Context, q xsql.Query, dest ...interface{}) error { return nil }}},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   want{o: managed.ExternalObservation{ResourceExists: true, ResourceLateInitialized: true, ResourceUpToDate: true}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Observe(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("%s\nobserve: -want error, +got error:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.o, got); diff != "" {
				t.Errorf("%s\nobserve: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	errBoom := errors.New("boom")
	type fields struct{ db xsql.DB }
	type args struct {
		ctx context.Context
		mg  resource.Managed
	}
	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   error
	}{
		"ErrNotPublication": {
			reason: "Error if managed resource is not Publication",
			args:   args{mg: nil},
			want:   errors.New(errNotPublication),
		},
		"ErrDropPublication": {
			reason: "Errors dropping publication should be returned",
			fields: fields{db: &mockDB{MockExec: func(ctx context.Context, q xsql.Query) error { return errBoom }}},
			args:   args{mg: &v1alpha1.Publication{}},
			want:   errors.Wrap(errBoom, errDropPublication),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			err := e.Delete(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want, err, test.EquateErrors()); diff != "" {
				t.Errorf("%s\ndelete: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
