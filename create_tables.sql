PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS "collections" ( 
	"id" integer, 
	"name" varchar NOT NULL, 
	"description" varchar, 
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS "telescopes" (
	"id" integer, 
	"name" varchar NOT NULL,
	PRIMARY KEY (id)
);



CREATE TABLE IF NOT EXISTS "obstypes" (
	"id" integer, 
	"type" varchar NOT NULL,
	PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS "pulsar_info" (
	"jname" varchar, 
	"ephemeris" varchar, 
	"template" varchar,
	"DM" varchar,
	"RM" varchar,
	"updated_at" datetime, 
	PRIMARY KEY (jname)
);

CREATE TABLE IF NOT EXISTS "processes" (
	"id" integer, 
	"name" varchar NOT NULL,
	"description" varchar,
	PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS "slurm_jobs"(
	"id" integer, 
	"file" varchar NOT NULL, 
	"submitted_at" datetime NOT NULL, 
	"status" varchar,  
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS "backends" (
	"id" integer, 
	"name" varchar NOT NULL,
	PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS "observations" ("id" integer,"filename" varchar NOT NULL,
	"source" varchar NOT NULL, 
	"start_utc" varchar NOT NULL,
	"start_mjd" varchar NOT NULL,
	"nchan" integer NOT NULL, 
	"nbin" integer NOT NULL, 
	"npol" integer NOT NULL, 
	"nsubint" integer NOT NULL, 
	"cfreq" real NOT NULL, 
	"bw" real NOT NULL, 
	"type" integer NOT NULL, 
	"telescope" integer NOT NULL, 
	"filesize" real NOT NULL, 
	"collection" integer NOT NULL, 
	"tobs" real NOT NULL,
	"backend" varchar NOT NULL,
	"state" varchar DEFAULT "INIT", 
	PRIMARY KEY (id),
	FOREIGN KEY(type) REFERENCES obstypes(id) ON DELETE CASCADE ON UPDATE CASCADE, 
	FOREIGN KEY(telescope) REFERENCES telescopes(id) ON DELETE CASCADE ON UPDATE CASCADE, 
	FOREIGN KEY(collection) REFERENCES collections(id) ON DELETE CASCADE ON UPDATE CASCADE
	FOREIGN KEY(backend) REFERENCES backends(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS "processings" (
"id" integer,
"obs_id" integer NOT NULL,
"process_id" integer NOT NULL, 
"slurm_job_id" integer NOT NULL, 
FOREIGN KEY(obs_id) REFERENCES observations(id) ON DELETE CASCADE ON UPDATE CASCADE, 
FOREIGN KEY(process_id) REFERENCES processes(id) ON DELETE CASCADE ON UPDATE CASCADE,
FOREIGN KEY(slurm_job_id) REFERENCES jobs(id) ON DELETE CASCADE ON UPDATE CASCADE,
PRIMARY KEY (id)
);



INSERT INTO telescopes (id, name) VALUES (NULL, "Parkes");
INSERT INTO backends (id, name) VALUES (NULL, "Medusa");
INSERT INTO obstypes (id, type) VALUES (NULL, "Pulsar");
INSERT INTO obstypes (id, type) VALUES (NULL, "PolnCal");
INSERT INTO obstypes (id, type) VALUES (NULL, "FluxCal");



