package v1alpha1

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reference"
	"github.com/pkg/errors"
)

// PublicationParameters are the configurable fields of a Publication.
type PublicationParameters struct {
	// Owner of the publication.
	// +optional
	// +crossplane:generate:reference:type=Role
	Owner *string `json:"owner,omitempty"`

	// OwnerRef references the role object this publication is owned by.
	// +immutable
	// +optional
	OwnerRef *xpv1.Reference `json:"ownerRef,omitempty"`

	// OwnerSelector selects a reference to a Role this publication is owned by.
	// +immutable
	// +optional
	OwnerSelector *xpv1.Selector `json:"ownerSelector,omitempty"`

	// Database this publication is for.
	// +optional
	// +crossplane:generate:reference:type=Database
	Database *string `json:"database,omitempty"`

	// DatabaseRef references the database object this publication is for.
	// +immutable
	// +optional
	DatabaseRef *xpv1.Reference `json:"databaseRef,omitempty"`

	// DatabaseSelector selects a reference to a Database this publication is for.
	// +immutable
	// +optional
	DatabaseSelector *xpv1.Selector `json:"databaseSelector,omitempty"`

	// AllTables publishes all tables in the database when true.
	// +optional
	AllTables *bool `json:"allTables,omitempty"`

	// Tables is the list of tables to include in the publication in
	// "schema.table" format.
	// +kubebuilder:validation:MinItems=1
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Publish is the list of DML operations to publish.
	// +kubebuilder:validation:Enum=insert;update;delete;truncate
	// +optional
	Publish []string `json:"publish,omitempty"`

	// PublishViaPartitionRoot defines whether changes in a partitioned table are
	// published using the identity of the partitioned table.
	// +optional
	PublishViaPartitionRoot *bool `json:"publishViaPartitionRoot,omitempty"`

	// DropCascade will drop dependent objects when deleting the publication.
	// +optional
	DropCascade *bool `json:"dropCascade,omitempty"`
}

// PublicationSpec defines the desired state of a Publication.
type PublicationSpec struct {
	xpv1.ResourceSpec `json:",inline"`
	ForProvider       PublicationParameters `json:"forProvider"`
}

// A PublicationStatus represents the observed state of a Publication.
type PublicationStatus struct {
	xpv1.ResourceStatus `json:",inline"`
}

// +kubebuilder:object:root=true

// A Publication represents the declarative state of a PostgreSQL Publication.
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="DATABASE",type="string",JSONPath=".spec.forProvider.database"
// +kubebuilder:printcolumn:name="OWNER",type="string",JSONPath=".spec.forProvider.owner"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,sql}
type Publication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PublicationSpec   `json:"spec"`
	Status PublicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PublicationList contains a list of Publication
type PublicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Publication `json:"items"`
}

// ResolveReferences of this Publication
func (mg *Publication) ResolveReferences(ctx context.Context, c client.Reader) error {
	r := reference.NewAPIResolver(c, mg)

	var rsp reference.ResolutionResponse
	var err error

	rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
		CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.Owner),
		Extract:      reference.ExternalName(),
		Reference:    mg.Spec.ForProvider.OwnerRef,
		Selector:     mg.Spec.ForProvider.OwnerSelector,
		To: reference.To{
			List:    &RoleList{},
			Managed: &Role{},
		},
	})
	if err != nil {
		return errors.Wrap(err, "mg.Spec.ForProvider.Owner")
	}
	mg.Spec.ForProvider.Owner = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.OwnerRef = rsp.ResolvedReference

	rsp, err = r.Resolve(ctx, reference.ResolutionRequest{
		CurrentValue: reference.FromPtrValue(mg.Spec.ForProvider.Database),
		Extract:      reference.ExternalName(),
		Reference:    mg.Spec.ForProvider.DatabaseRef,
		Selector:     mg.Spec.ForProvider.DatabaseSelector,
		To: reference.To{
			List:    &DatabaseList{},
			Managed: &Database{},
		},
	})
	if err != nil {
		return errors.Wrap(err, "mg.Spec.ForProvider.Database")
	}
	mg.Spec.ForProvider.Database = reference.ToPtrValue(rsp.ResolvedValue)
	mg.Spec.ForProvider.DatabaseRef = rsp.ResolvedReference

	return nil
}
