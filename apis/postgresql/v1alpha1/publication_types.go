package v1alpha1

import (
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PublicationPublish represents which operations are published by a publication.
type PublicationPublish struct {
	// Insert publishes INSERT operations.
	// +optional
	Insert *bool `json:"insert,omitempty"`

	// Update publishes UPDATE operations.
	// +optional
	Update *bool `json:"update,omitempty"`

	// Delete publishes DELETE operations.
	// +optional
	Delete *bool `json:"delete,omitempty"`

	// Truncate publishes TRUNCATE operations.
	// +optional
	Truncate *bool `json:"truncate,omitempty"`
}

// PublicationParameters define the desired state of a PostgreSQL publication.
type PublicationParameters struct {
	// Database this publication belongs to.
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

	// AllTables when true publishes all tables in the database.
	// +optional
	AllTables *bool `json:"allTables,omitempty"`

	// Tables that will be published when AllTables is false.
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Publish determines which DML operations are published.
	// +optional
	Publish *PublicationPublish `json:"publish,omitempty"`

	// PublishViaPartitionRoot determines whether partition changes are published
	// using the root partition identity.
	// +optional
	PublishViaPartitionRoot *bool `json:"publishViaPartitionRoot,omitempty"`
}

// PublicationSpec defines the desired state of a Publication.
type PublicationSpec struct {
	xpv1.ResourceSpec `json:",inline"`
	ForProvider       PublicationParameters `json:"forProvider"`
}

// PublicationStatus represents the observed state of a Publication.
type PublicationStatus struct {
	xpv1.ResourceStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="DATABASE",type="string",JSONPath=".spec.forProvider.database"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,sql}
// A Publication represents the declarative state of a PostgreSQL publication.
type Publication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PublicationSpec   `json:"spec"`
	Status PublicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// PublicationList contains a list of Publication.
type PublicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Publication `json:"items"`
}
