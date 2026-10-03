package schema

import (
	"github.com/Wei-Shaw/sub2api/ent/schema/mixins"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// EgressIdentity is an immutable normalized public IP capacity identity.
type EgressIdentity struct {
	ent.Schema
}

func (EgressIdentity) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "egress_identities"},
	}
}

func (EgressIdentity) Mixin() []ent.Mixin {
	return []ent.Mixin{mixins.TimeMixin{}}
}

func (EgressIdentity) Fields() []ent.Field {
	return []ent.Field{
		field.String("public_ip").
			NotEmpty().
			SchemaType(map[string]string{dialect.Postgres: "inet"}),
		field.Enum("status").
			Values("active", "retired").
			Default("active"),
	}
}

func (EgressIdentity) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("routes", EgressRoute.Type).Ref("expected_identity"),
	}
}

func (EgressIdentity) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("public_ip").Unique(),
		index.Fields("status"),
	}
}
