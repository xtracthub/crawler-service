CREATE TABLE users (
id serial PRIMARY KEY,
email varchar(50) not null,
globus_id varchar(50) not null DEFAULT 'None',
first_name varchar(50) not null,
last_name varchar(50) not null
);

CREATE TABLE files (
id serial PRIMARY KEY,
filename varchar(50) NOT NULL,
fullpath varchar(50) NOT NULL,
user_id INTEGER REFERENCES users (id) NOT NULL
);

CREATE TABLE metadata (
id serial PRIMARY KEY,
file_id INTEGER REFERENCES files (id) NOT NULL,
extension varchar(50) NOT NULL,
size_mb integer NOT NULL,
metadata json NOT NULL,
last_collected TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tasks (
id serial PRIMARY KEY,
task_uuid varchar(50) NOT NULL,
user_id INTEGER REFERENCES files (id) NOT NULL,
status varchar(50) NOT NULL,
starttime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
endttime TIMESTAMP
);