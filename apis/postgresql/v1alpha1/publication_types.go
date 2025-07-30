/*
Copyright 2024 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
)

// PublicationOperation represents a DML operation that can be published.
// +kubebuilder:validation:Enum=insert;update;delete;truncate
type PublicationOperation string

const (
	// PublishInsert indicates INSERT operations are published.
	PublishInsert PublicationOperation = "insert"
	// PublishUpdate indicates UPDATE operations are published.
	PublishUpdate PublicationOperation = "update"
	// PublishDelete indicates DELETE operations are published.
	PublishDelete PublicationOperation = "delete"
	// PublishTruncate indicates TRUNCATE operations are published.
	PublishTruncate PublicationOperation = "truncate"
)

// PublicationParameters are the configurable fields of a Publication.
type PublicationParameters struct {
	// Owner is the role that owns this publication.
	// +optional
	// +crossplane:generate:reference:type=Role
	Owner *string `json:"owner,omitempty"`

	// OwnerRef references the role object that owns this publication.
	// +immutable
	// +optional
	OwnerRef *xpv1.Reference `json:"ownerRef,omitempty"`

	// OwnerSelector selects a reference to a Role that owns this publication.
	// +immutable
	// +optional
	OwnerSelector *xpv1.Selector `json:"ownerSelector,omitempty"`

	// Database this publication is defined in.
	// +optional
	// +crossplane:generate:reference:type=Database
	Database *string `json:"database,omitempty"`

	// DatabaseRef references the database this publication is for.
	// +immutable
	// +optional
	DatabaseRef *xpv1.Reference `json:"databaseRef,omitempty"`

	// DatabaseSelector selects a reference to a Database this publication is for.
	// +immutable
	// +optional
	DatabaseSelector *xpv1.Selector `json:"databaseSelector,omitempty"`

	// AllTables indicates whether all tables should be published.
	// +optional
	AllTables *bool `json:"allTables,omitempty"`

	// Tables lists tables included in the publication.
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Publish defines which DML operations will be published.
	// +optional
	Publish []PublicationOperation `json:"publish,omitempty"`

	// PublishViaPartitionRoot enables publish_via_partition_root parameter.
	// +optional
	PublishViaPartitionRoot *bool `json:"publishViaPartitionRoot,omitempty"`

	// DropCascade drops dependent objects when deleting the publication.
	// +optional
	DropCascade *bool `json:"dropCascade,omitempty"`
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
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="DATABASE",type="string",JSONPath=".spec.forProvider.database"
// +kubebuilder:printcolumn:name="OWNER",type="string",JSONPath=".spec.forProvider.owner"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,sql}
// Publication represents the declarative state of a PostgreSQL publication.
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
