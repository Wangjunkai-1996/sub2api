package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// AccountEgressBinding stores the ordered routes selected for one account.
type AccountEgressBinding struct {
	ent.Schema
}

func (AccountEgressBinding) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "account_egress_bindings"},
		field.ID("account_id", "route_id"),
	}
}

func (AccountEgressBinding) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("account_id"),
		field.Int64("route_id"),
		field.Int("position").Default(0).Min(0),
		field.Bool("is_primary").Default(false),
		field.Enum("status").Values("active", "draining").Default("active"),
		field.Time("created_at").
			Immutable().
			Default(time.Now).
			SchemaType(map[string]string{dialect.Postgres: "timestamptz"}),
		field.Time("updated_at").
			Default(time.Now).
			UpdateDefault(time.Now).
			SchemaType(map[string]string{dialect.Postgres: "timestamptz"}),
	}
}

func (AccountEgressBinding) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("account", Account.Type).
			Field("account_id").
			Unique().
			Required().
			Annotations(entsql.OnDelete(entsql.Cascade)),
		edge.To("route", EgressRoute.Type).
			Field("route_id").
			Unique().
			Required().
			Annotations(entsql.OnDelete(entsql.Restrict)),
	}
}

func (AccountEgressBinding) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("route_id"),
		index.Fields("account_id", "position").Unique(),
		index.Fields("account_id").
			Unique().
			Annotations(entsql.IndexWhere("is_primary")),
	}
}
