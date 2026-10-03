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

// EgressRoute is a proxy-backed or deployment-scoped direct path to an egress identity.
type EgressRoute struct {
	ent.Schema
}

func (EgressRoute) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "egress_routes"},
	}
}

func (EgressRoute) Mixin() []ent.Mixin {
	return []ent.Mixin{mixins.TimeMixin{}}
}

func (EgressRoute) Fields() []ent.Field {
	return []ent.Field{
		field.Enum("kind").Values("proxy", "direct"),
		field.Int64("proxy_id").Optional().Nillable(),
		field.String("runtime_scope").Optional().Nillable().MaxLen(128),
		field.Int64("expected_identity_id").Optional().Nillable(),
		field.Enum("state").
			Values("pending_verification", "active", "inactive", "expired", "identity_mismatch", "retired").
			Default("pending_verification"),
		field.String("last_observed_ip").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "inet"}),
		field.Time("last_probed_at").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "timestamptz"}),
		field.Time("verified_at").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "timestamptz"}),
		field.Int64("revision").Default(1).Min(1),
		field.String("last_error").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
	}
}

func (EgressRoute) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("proxy", Proxy.Type).
			Field("proxy_id").
			Unique().
			Annotations(entsql.OnDelete(entsql.Restrict)),
		edge.To("expected_identity", EgressIdentity.Type).
			Field("expected_identity_id").
			Unique().
			Annotations(entsql.OnDelete(entsql.Restrict)),
		edge.From("accounts", Account.Type).
			Ref("egress_routes").
			Through("bindings", AccountEgressBinding.Type),
	}
}

func (EgressRoute) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("proxy_id").Unique(),
		index.Fields("runtime_scope").Unique(),
		index.Fields("expected_identity_id"),
		index.Fields("state", "expected_identity_id"),
	}
}
