use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn basic_select1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select u.id, u.username, emailadres from users as u
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "emailadres"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true])
}

#[test]
pub fn basic_left_join() {
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
            u.id,
            u.username,
            u.emailadres,
            pets.pet_id,
            pets.pet_name
        from
            users as u
        left join
            pets
        on
            pets.pet_id = u.pet_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "emailadres", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true])
}

#[test]
pub fn basic_double_left_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true)
        .push_column("plant_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.id,
            u.username,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            pets.pet_id = users.pet_id
        left join
            plants as pl
        on
            plants.plant_id = users.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join_1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join_2() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", false);

    let orders_table = Table::new("orders")
        .push_column("order_id", false)
        .push_column("user_id", false)
        .push_column("product_id", false)
        .push_column("order_date", false);

    let products_table = Table::new("products")
        .push_column("product_id", false)
        .push_column("product_name", false)
        .push_column("price", false);

    let source = Source::new(vec![user_table, orders_table, products_table]);

    let query = r#"
        select
            u.user_id,
            u.name as user_name,
            u.emailadres,
            o.order_id,
            o.order_date,
            p.product_name,
            p.price
        from
            users u
        left join
            orders o on u.user_id = o.user_id
        inner join
            products p on o.product_id = p.product_id;

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "user_id",
        "user_name",
        "emailadres",
        "order_id",
        "order_date",
        "product_name",
        "price",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join_4() {
    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false)
        .push_column("location_id", false);

    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", false);

    let products_table = Table::new("location")
        .push_column("location_id", false)
        .push_column("street_name", false);

    let source = Source::new(vec![user_table, orders_table, products_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
            u.name,
            u.emailadres,
            l.location_id,
            l.street_name
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
        left join
            location l on a.location_id = l.location_id

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "name",
        "emailadres",
        "location_id",
        "street_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join_5() {
    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false)
        .push_column("location_id", false);

    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", false);

    let products_table = Table::new("location")
        .push_column("location_id", false)
        .push_column("street_name", false);

    let source = Source::new(vec![user_table, orders_table, products_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
            u.name,
            u.emailadres,
            l.location_id,
            l.street_name
        from
            agenda a
        left join
            users u on a.user_id = u.user_id
        inner join
            location l on a.location_id = l.location_id

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "name",
        "emailadres",
        "location_id",
        "street_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, false, false])
}

#[test]
pub fn basic_double_left_inner_join_6() {
    let orders_table = Table::new("agenda")
        .push_column("agenda_id", false)
        .push_column("startdate", false)
        .push_column("user_id", false)
        .push_column("location_id", false);

    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true);

    let products_table = Table::new("location")
        .push_column("location_id", false)
        .push_column("street_name", false);

    let source = Source::new(vec![user_table, orders_table, products_table]);

    let query = r#"
        select
            a.agenda_id,
            a.startdate,
            u.user_id,
            u.name,
            u.emailadres,
            l.location_id,
            l.street_name
        from
            agenda a
        inner join
            users u on a.user_id = u.user_id
        inner join
            location l on a.location_id = l.location_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "agenda_id",
        "startdate",
        "user_id",
        "name",
        "emailadres",
        "location_id",
        "street_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false, true, false, false])
}

#[test]
pub fn select_count1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            count(users.id) as user_count
        from
            users
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["user_count"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_cast3() {
    let source = Source::empty();

    let query = r#"
        SELECT NULL::INTEGER as nullable;

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["nullable"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}
