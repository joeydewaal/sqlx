use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn join_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
left join
	pets using (pet_id)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true])
}

#[test]
pub fn natural_join_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
natural join
	pets
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false])
}

#[test]
pub fn natural_join_2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
natural left join
	pets
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true])
}

#[test]
pub fn join_full_outer_2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.pet_id,
	pets.pet_name
from
	users
full outer join
	pets using (pet_id)
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true])
}

#[test]
pub fn full_outer_join_2() {
    let color_table = Table::new("colors")
        .push_column("color_uuid", false)
        .push_column("color", true);

    let objects_table = Table::new("objects")
        .push_column("object_uuid", false)
        .push_column("colors", true);

    let source = Source::new(vec![color_table, objects_table]);

    let query = r#"
    SELECT o.object_uuid, o.colors,
    c1.color_uuid as color1_uuid, c1.color as color1_color,
    c2.color_uuid as color2_uuid, c2.color as color2_color
    FROM objects o
    FULL OUTER JOIN colors c1 ON c1.color_uuid = o.colors[1]
    LEFT JOIN colors c2 ON c2.color_uuid = o.colors[2]
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "object_uuid",
        "colors",
        "color1_uuid",
        "color1_color",
        "color2_uuid",
        "color2_color",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true, true, true])
}

#[test]
pub fn full_outer_join_3() {
    let color_table = Table::new("colors")
        .push_column("color_uuid", false)
        .push_column("color", true);

    let objects_table = Table::new("objects")
        .push_column("object_uuid", false)
        .push_column("colors", true);

    let source = Source::new(vec![color_table, objects_table]);

    let query = r#"
    SELECT o.object_uuid, o.colors,
    c1.color_uuid as color1_uuid, c1.color as color1_color,
    c2.color_uuid as color2_uuid, c2.color as color2_color
    FROM objects o
    FULL OUTER JOIN colors c1 ON c1.color_uuid = o.colors[1]
    INNER JOIN colors c2 ON c2.color_uuid = o.colors[2]

    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "object_uuid",
        "colors",
        "color1_uuid",
        "color1_color",
        "color2_uuid",
        "color2_color",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true, true, true, false, true])
}

#[test]
pub fn cross_join_3() {
    let table_1 = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("pet_id", true);

    let table_2 = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![table_1, table_2]);

    let query = r#"
select
	users.*,
	pets.*,
	pets2.*
from
	users
cross join pets
left join pets pets2 on pets2.pet_id = users.pet_id
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id", "username", "pet_id", "pet_id", "pet_name", "pet_id", "pet_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, false, false, true, true])
}

#[test]
pub fn cross_join_4() {
    let table_1 = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("pet_id", true);

    let table_2 = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![table_1, table_2]);

    let query = r#"
select
	users.*,
	pets.*,
	pets2.*
from
	users
cross join pets
right join pets pets2 on pets2.pet_id = users.pet_id
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id", "username", "pet_id", "pet_id", "pet_name", "pet_id", "pet_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true, true, false, false])
}

#[test]
pub fn cross_join_5() {
    let table_1 = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("pet_id", true);

    let table_2 = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![table_1, table_2]);

    let query = r#"
select
	users.*,
	pets.*,
	pets2.*
from
	users
cross join pets
full outer join pets pets2 on pets2.pet_id = users.pet_id
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id", "username", "pet_id", "pet_id", "pet_name", "pet_id", "pet_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true, true, true, true])
}
