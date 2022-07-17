CREATE TABLE calls (
    date DATE,
    call_id VARCHAR,
    audio_file VARCHAR,
    oper_id INTEGER
);

CREATE TABLE dates (
    date DATE
);

CREATE TABLE stat_total (
    date DATE,
    count INTEGER,
    stat_date_time DATE
);

CREATE TABLE stat_oper (
    date DATE,
    oper_id INTEGER,
    count INTEGER,
    stat_date_time DATE
);

INSERT INTO calls VALUES
    ('2022-01-20', 'ERCERCER', 'rec_12536123.wav', '134'),
    ('2022-01-20', '34FEWEC3', 'rec_89372934.wav', '134'),
    ('2022-01-20', 'ERC34F3E', 'rec_56756775.wav', '134'),
    ('2022-01-20', 'ERCJNER8', 'rec_32454565.wav', '128'),
    ('2022-01-20', 'ERLHCRE8', 'rec_34545567.wav', '125'),
    ('2022-01-20', 'LKECRE9C', 'rec_23434564.wav', '125'),
    ('2022-01-19', 'LJC8ER24', 'rec_65778978.wav', '127'),
    ('2022-01-19', 'KJNDFC94', 'rec_34545766.wav', '128'),
    ('2022-01-19', 'KJDC9833', 'rec_34545656.wav', '125'),
    ('2022-01-19', 'JHB38743', 'rec_23434545.wav', '125'),
    ('2022-01-19', 'U7JH76H5', 'rec_56767876.wav', '127'),
    ('2022-01-18', '34F345F4', 'rec_56567678.wav', '134'),
    ('2022-01-18', 'WED34F45', 'rec_34534534.wav', '134'),
    ('2022-01-18', 'W3D34F56', 'rec_56756767.wav', '134'),
    ('2022-01-18', 'WF435F55', 'rec_23434534.wav', '116'),
    ('2022-01-17', 'NKDBUS83', 'rec_13434876.wav', '134'),
    ('2022-01-17', 'NBE83642', 'rec_13434468.wav', '116'),
    ('2022-01-17', 'NVID49DF', 'rec_13434111.wav', '134');

INSERT INTO dates VALUES
    ('2022-01-20'),
    ('2022-01-18');