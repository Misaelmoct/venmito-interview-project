CREATE TABLE persons (
    person_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(32) NOT NULL,
    last_name VARCHAR(32) NOT NULL,
    email_address VARCHAR(128) NOT NULL,
    telephone VARCHAR(11) NOT NULL,
    city VARCHAR(32) NOT NULL,
    country VARCHAR(32) NOT NULL,
    is_valid boolean DEFAULT TRUE
);

CREATE TABLE promotions (
    promotion_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    person_id INT NOT NULL,
    promotion VARCHAR(32),
    responded boolean DEFAULT FALSE,
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
);

CREATE TABLE transfers (
    transfer_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    sender_id INT NOT NULL,
    recipient_id INT NOT NULL,
    amount DECIMAL NOT NULL,
    transfer_date DATE NOT NULL,
    FOREIGN KEY (sender_id) REFERENCES persons(person_id),
    FOREIGN KEY (recipient_id) REFERENCES persons(person_id)
);

CREATE TABLE stores (
    store_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    store_name VARCHAR(64) NOT NULL
);

CREATE TABLE transactions (
    transaction_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    person_id INT NOT NULL,
    store_id INT NOT NULL,
    item VARCHAR(32) NOT NULL,
    price DECIMAL NOT NULL,
    transaction_date DATE NOT NULL,
    FOREIGN KEY (person_id) REFERENCES persons(person_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);