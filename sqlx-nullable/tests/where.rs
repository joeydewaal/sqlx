use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn where1() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            u.emailadres
        from
            users u
        where
            u.emailadres is not null
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["user_id", "name", "emailadres"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false])
}

#[test]
pub fn where3() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where u.emailadres is not null
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["agenda_id", "startdate", "user_id", "emailadres", "age"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, true])
}

#[test]
pub fn where4() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where u.emailadres is not null and age is not null

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["agenda_id", "startdate", "user_id", "emailadres", "age"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, false])
}

#[test]
pub fn where5() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where u.emailadres is not null and age is not null and name is not null

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["agenda_id", "startdate", "user_id", "emailadres", "age"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, false])
}

#[test]
pub fn where6() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where age < 15
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["agenda_id", "startdate", "user_id", "emailadres", "age"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, true, false])
}

#[test]
pub fn where7() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where age < 15 or a.agenda_id = 1
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["agenda_id", "startdate", "user_id", "emailadres", "age"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true])
}

#[test]
pub fn where8() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true);

    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false);

    let source = Source::new(vec![user_table, orders_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
			u.emailadres,
            u.age
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
		where age < 15 and a.agenda_id = 1
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["agenda_id", "startdate", "user_id", "emailadres", "age"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, true, false])
}

#[test]
pub fn where9() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            p.pet_id = u.pet_id
        left join
            plants as pl
        on
            pl.plant_id = p.plant_id
        where
            p.pet_name is not null
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "name",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, true, true])
}

#[test]
pub fn where10() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            p.pet_id = u.pet_id
        left join
            plants as pl
        on
            pl.plant_id = p.plant_id
        where
            pl.plant_name is not null
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "name",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, false, false])
}

#[test]
pub fn where11() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            p.pet_id = u.pet_id
        right join
            plants as pl
        on
            pl.plant_id = p.plant_id
        where
            pl.plant_name is not null
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "name",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true, false, false])
}

#[test]
pub fn where12() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            p.pet_id = u.pet_id
        right join
            plants as pl
        on
            pl.plant_id = p.plant_id
        where
            p.pet_name is not null
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "name",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, false, false, false, false])
}

#[test]
pub fn where13() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            p.pet_id = u.pet_id
        right join
            plants as pl
        on
            pl.plant_id = p.plant_id
        where
            u.user_id is not null
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "name",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, false, false])
}
