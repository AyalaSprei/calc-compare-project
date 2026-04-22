CREATE TABLE t_data (
    data_id INT IDENTITY(1,1) PRIMARY KEY,
    a FLOAT NOT NULL,
    b FLOAT NOT NULL,
    c FLOAT NOT NULL,
    d FLOAT NOT NULL
);

CREATE TABLE t_targil (
    targil_id INT IDENTITY(1,1) PRIMARY KEY,
    targil VARCHAR(MAX) NOT NULL,
    tnai VARCHAR(MAX) NULL,
    targil_false VARCHAR(MAX) NULL
);

CREATE TABLE t_results (
    results_id INT IDENTITY(1,1) PRIMARY KEY,
    data_id INT NOT NULL REFERENCES t_data(data_id),     
    targil_id INT NOT NULL REFERENCES t_targil(targil_id), 
    method VARCHAR(50) NOT NULL,                     
    result FLOAT NULL
);

CREATE TABLE t_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    targil_id INT NOT NULL REFERENCES t_targil(targil_id),
    method VARCHAR(50) NOT NULL,
	run_time FLOAT NOT NULL
);