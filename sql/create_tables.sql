DROP TABLE IF EXISTS cities CASCADE;
DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE events (
    event_id VARCHAR(20) PRIMARY KEY,
    event_name VARCHAR(150) NOT NULL,
    city VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    base_ticket_price NUMERIC(10,2) NOT NULL
);

INSERT INTO cities (city_name) VALUES
('Aveiro'),
('Porto'),
('Lisboa'),
('Coimbra'),
('Braga');

INSERT INTO events (event_id, event_name, city, category, base_ticket_price) VALUES
('E1', 'Tech Conference 2026', 'Aveiro', 'Technology', 40.00),
('E2', 'Rock Festival', 'Porto', 'Music', 55.00),
('E3', 'Football Match', 'Lisboa', 'Sports', 30.00),
('E4', 'Comedy Night', 'Coimbra', 'Entertainment', 25.00),
('E5', 'Jazz Festival', 'Braga', 'Music', 35.00);