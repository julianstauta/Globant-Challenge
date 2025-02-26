CREATE TABLE hired_employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    datetime TIMESTAMP NOT NULL,
    department_id INTEGER REFERENCES departments(id),
    job_id INTEGER REFERENCES jobs(id)
);

CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    department VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    job VARCHAR(255) UNIQUE NOT NULL
);