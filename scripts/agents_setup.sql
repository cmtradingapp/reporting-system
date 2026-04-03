-- ============================================================
-- AGENTS MASTER + TARGETS HISTORY SETUP
-- Run: psql -U postgres -d datawarehouse -f agents_setup.sql
-- Safe to re-run (idempotent)
-- ============================================================

-- STEP 1: Tables

CREATE TABLE IF NOT EXISTS agents_master (
    id           SERIAL PRIMARY KEY,
    agent_name   TEXT NOT NULL,
    office_name  TEXT,
    email        TEXT UNIQUE,
    start_date   DATE,
    target_group TEXT NOT NULL,
    crm_user_id  INTEGER,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_targets_history (
    id                     SERIAL PRIMARY KEY,
    agent_name             TEXT NOT NULL,
    office_name            TEXT,
    email                  TEXT,
    start_date             DATE,
    target_group           TEXT NOT NULL,
    crm_user_id            INTEGER,
    report_month           DATE NOT NULL,
    tenure_months          INTEGER NOT NULL,
    monthly_net_target     INTEGER,
    monthly_ftd100_target  INTEGER,
    created_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_agent_month UNIQUE (email, report_month)
);

CREATE INDEX IF NOT EXISTS idx_ath_email        ON agent_targets_history (email);
CREATE INDEX IF NOT EXISTS idx_ath_report_month ON agent_targets_history (report_month);
CREATE INDEX IF NOT EXISTS idx_ath_crm_user     ON agent_targets_history (crm_user_id);

-- STEP 2: Agent data
-- Resolved duplicates:
--   jane.j@cmtrading.com     -> Jane Jochabed (Makama Jochabed removed)
--   joseph.a@cmtrading.com   -> Joseph Agusiobo (corrected email)
--   victoria.j@cmtrading.com -> Victoria Jatau
-- No email (removed): Matthew (LAG Nigeria, 2026-04-01)

INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Titilayo.O','LAG Nigeria','titilayo.o@cmtrading.com','2025-05-14','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Osazuwa.O','LAG Nigeria','osazuwa.o@cmtrading.com','2025-05-14','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Elizabeth A','LAG Nigeria','elizabeth.a@cmtrading.com','2025-06-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Gloria C','LAG Nigeria','glory.c@cmtrading.com','2025-06-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Vera Domingo','LAG Nigeria','veronica.d@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Philip I','LAG Nigeria','phillip.i@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ope J','LAG Nigeria','ope.j@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joy E','LAG Nigeria','joy.e@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jimoh D','LAG Nigeria','jimoh.d@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Agatha O','LAG Nigeria','agatha.o@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Karim K','GMT','karim.k@cmtrading.com','2023-12-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Alaa L','GMT','alaa.l@cmtrading.com','2024-02-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Pascale R','GMT','pascale.r@cmtrading.com','2024-05-02','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Samar M','GMT','samar.m@cmtrading.com','2024-05-02','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Roudy','GMT','roudy.g@cmtrading.com','2025-01-03','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Brigitta','GMT','brigitta.k@cmtrading.com','2025-01-03','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nour H','GMT','nour.h@cmtrading.com','2025-09-12','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ornela Z','GMT','ornela.z@cmtrading.com','2025-11-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joanna K','GMT','joanna.k@cmtrading.com','2025-11-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Gio F','GMT','gio.f@cmtrading.com','2026-02-23','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Tony Ho','GMT','tony.ho@cmtrading.com','2026-03-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Laya M','GMT','laya.m@cmtrading.com','2026-03-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Charbel S','GMT','charbel.s@cmtrading.com','2026-03-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('George Ab','GMT','george.ab@cmtrading.com','2026-03-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Noel D','GMT','noel.d@cmtrading.com','2023-10-02','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Victoria D','Cyprus','victoria.da@cmtrading.com','2023-12-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Yazan T','Cyprus','yazan.tai@cmtrading.com','2025-02-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Elvis','Cyprus','elvis.i@cmtrading.com','2025-04-22','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Junaid Rasheed','Cyprus','junaid.r@cmtrading.com','2025-08-11','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Syed Mansoor','Cyprus','syed.m@cmtrading.com','2025-08-11','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Marios Konstantinou','Cyprus','marios.k@cmtrading.com','2025-09-15','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Armen Mkhittaryan','Cyprus','armen.m@cmtrading.com','2025-09-15','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mark Stepanian','Cyprus','mark.ste@cmtrading.com','2025-10-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Georgios T','Cyprus','georgios.t@cmtrading.com','2025-11-03','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Cristian C','Cyprus','cristian.c@cmtrading.com','2026-02-09','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Christopher A','Cyprus','christopher.a@cmtrading.com','2026-02-12','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Oleksandra O','Cyprus','oleksandra.o@cmtrading.com','2026-03-30','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Emmanue Ike','Klinsman - ABJ - Nigeria','emmanuel.i@cmtrading.com','2024-08-22','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Emmanuel.U','Klinsman - ABJ - Nigeria','emmanuel.u@cmtrading.com','2025-11-18','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Benjamin Willie','Klinsman - ABJ - Nigeria','benjamin.w@cmtrading.com','2024-08-22','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('George U','Klinsman - ABJ - Nigeria','george.u@cmtrading.com','2025-10-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joshua S','Klinsman - ABJ - Nigeria','joshua.s@cmtrading.com','2025-10-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Johnson A','Klinsman - ABJ - Nigeria','johnson.a@cmtrading.com','2025-11-13','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Felix T','Klinsman - ABJ - Nigeria','felix.t@cmtrading.com','2026-03-03','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mmekomabasi O','Klinsman - ABJ - Nigeria','mmekomabasi.o@cmtrading.com','2025-12-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Daniel N','Klinsman - ABJ - Nigeria','daniel.n@cmtrading.com','2025-12-08','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Emmanuel O','Klinsman - ABJ - Nigeria','emmanuel.o@cmtrading.com','2026-03-02','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Goodness E','Yusuf- ABJ - Nigeria','goodness.e@cmtrading.com','2025-10-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jerome T','Yusuf- ABJ - Nigeria','jerome.t@cmtrading.com','2025-11-05','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jeremiah A','Yusuf- ABJ - Nigeria','jeremiah.a@cmtrading.com','2025-11-13','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Bashir H','Yusuf- ABJ - Nigeria','bashir.h@cmtrading.com','2026-01-19','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Funmilola S','Yusuf- ABJ - Nigeria','funmilola.s@cmtrading.com','2026-01-19','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Amedeo Giacomini','BULGARIA','amedeo.g@cmtrading.com','2025-09-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Vladimir Georgiev','Galin -Team 2- Bulgaria','vladimir.g@cmtrading.com','2025-09-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Plamen Petrov','Galin -Team 2- Bulgaria','plamen.p@cmtrading.com','2025-09-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Danail Ganchev','Galin -Team 2- Bulgaria','danail.g@cmtrading.com','2025-09-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Boris P','Galin -Team 2- Bulgaria','boris.p@cmtrading.com','2025-11-05','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Tsvetan T','Galin -Team 2- Bulgaria','tsvetan.t@cmtrading.com','2025-11-11','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Dobrin Velchev','Galin -Team 2- Bulgaria','dobrin.v@cmtrading.com','2025-11-26','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Bozhidar','Galin -Team 2- Bulgaria','bozhidar.h@cmtrading.com','2026-01-19','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Georgi G','Galin -Team 2- Bulgaria','georgi.g@cmtrading.com','2026-04-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Svetoslav Bozhilov','Krasi P-Team 2- Bulgaria','svetoslav.b@cmtrading.com','2025-09-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Valeri Kablan','Krasi P-Team 2- Bulgaria','valeri.k@cmtrading.com','2025-09-01','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Daniel T','Krasi P-Team 2- Bulgaria','daniel.t@cmtrading.com','2025-10-22','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ivan V','Krasi P-Team 2- Bulgaria','ivan.v@cmtrading.com','2025-11-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Monika Boteva','Krasi P-Team 2- Bulgaria','monika.b@cmtrading.com','2025-11-17','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Aleksandar O','Krasi P-Team 2- Bulgaria','aleksandar.o@cmtrading.com','2025-12-10','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Iskren Petrov','Krasi P-Team 2- Bulgaria','iskren.p@cmtrading.com','2025-12-10','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Martin Rogev','Krasi P-Team 2- Bulgaria','martin.r@cmtrading.com','2025-11-26','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('David Georgiev','Krasi P-Team 2- Bulgaria','david.g@cmtrading.com','2026-01-05','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Enriko A','Krasi P-Team 2- Bulgaria','enriko.a@cmtrading.com','2026-03-16','FTD100') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mbali Zinyanga','SA','mbali.z@cmtrading.com','2025-04-02','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mahlatshe Singo','SA','mahlatshe.s@cmtrading.com','2024-02-12','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nathan Naiker','SA','nathan.n@cmtrading.com','2025-06-10','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Quinton Robiyana','SA','nikita.r@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Gontse Molewa','SA','anele.m@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Anele Mdluli','SA','akhona.k@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nikita Ratiba','SA','princess.t@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ayanda Mohaika','SA','quinton.r@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nosipho Mthimukulu','SA','andile.r@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Akhona Kulata','SA','gontse.m@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Banele Xaba','SA','alfred.g@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('David Dalubuhle Ndebele','SA','banele.x@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Thembisile Tshabalala','SA','david.nd@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Katleho Moshesha','SA','nhlanhla.m@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Princess Themba','SA','nosipho.m@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Andile Radebe','SA','katleho.m@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Alfred Gordon','SA','thembisile.t@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nhlanhla Mazibuko','SA','ayanda.m@cmtrading.com','2026-03-17','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('DAHUNSI D TEMITOPE','LAG','temitope.d@cmtrading.com','2023-02-05','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Tamara Gbeinbo','LAG','tamara.g@cmtrading.com','2023-01-11','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ocheme Christianah Onyemowo','LAG','christianah.o@cmtrading.com','2023-04-12','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Bassey Christiana Ekpo','LAG','idong.a@cmtrading.com','2024-11-06','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nzekwe Ijeoma Oluchukwu','LAG','ijeoma.n@cmtrading.com','2024-09-09','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Chinemerem Nze Ruth','LAG','ruth.n@cmtrading.com','2024-09-04','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Eriba Grace Ugwoma','LAG','grace.er@cmtrading.com','2024-03-12','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Nduonyi Godwin O.','LAG','godwin.n@cmtrading.com','2025-07-04','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Kwushue, Nnamdi Anthony','LAG','nnamdi.k@cmtrading.com','2025-02-06','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joy  Valentina Okafor','LAG','joy.v@cmtrading.com','2025-03-11','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ene Cletus','LAG','ene.c@cmtrading.com','2025-11-24','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jennifer Owhuo','LAG','jennifer.o@cmtrading.com','2025-11-24','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Alagbe Ibukun Epaphras','LAG','epaphras.i@cmtrading.com','2026-09-02','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Akinlaja Temitope Olukorede','LAG','korede.a@cmtrading.com','2026-02-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Emmanuella Oluwasemilogo Osisanya','LAG','emmanuella.o@cmtrading.com','2026-02-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Agboola Funke Ifeoluwa','LAG','ife.a@cmtrading.com','2026-02-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Funmike sileola','LAG','funmike.s@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Edward christian','LAG','edward.c@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Paul Obiora','LAG','paul.o@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Victoria obidiran','LAG','victoria.o@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Marvellous akosile','LAG','marvellous.a@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Michael balogun','LAG','michael.ba@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Victoria nnaji','LAG','victoria.n@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Tayo logun','LAG','tayo.l@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Wilson Okafor','LAG','wilson.o@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Leonard james','LAG','leonard.j@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Monedo Tega','ABU','tega.m@cmtrading.com','2025-07-04','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Funtsi Rimamnyang','ABU','funtsi.r@cmtrading.com','2025-02-10','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Arikpo Peace','ABU','peace.a@cmtrading.com','2025-09-22','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Princess Adaugo Chidi','ABU','princess.c@cmtrading.com','2025-09-10','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Chilaka Juliet Amarachi','ABU','juliet.c@cmtrading.com','2025-06-11','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Faith Stephen','ABU','faith.s@cmtrading.com','2025-06-11','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jane Jochabed','ABU','jane.j@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Dede Lolia','ABU','dede.l@cmtrading.com','2026-01-14','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('David Nwamara','ABU','david.n@cmtrading.com','2026-01-14','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Alao Jessica','ABU','jessica.a@cmtrading.com','2026-01-14','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Shaure Racha','ABU','shaure.r@cmtrading.com','2026-01-14','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Abasiekeme Samson','ABU','abasiekeme.s@cmtrading.com','2026-01-14','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jane Okezi-dovie','ABU','jane.o@cmtrading.com','2025-11-06','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Success Elijah','ABU','success.el@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Christian Nnadozie','ABU','christian.n@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joseph Agusiobo','ABU','joseph.a@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Victoria Jatau','ABU','victoria.j@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Esther Jacob','ABU','esther.j@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Divine Uzoama','ABU','divine.u@cmtrading.com','2026-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Divine Chukwuendu','ABU','divine.c@cmtrading.com','2026-03-26','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Amaka Cynthia Agbawodike ','ABU','amaka.c@cmtrading.com','2026-03-26','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Bariton Peace Nzaga','ABU','baritone.p@cmtrading.com','2026-03-26','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ruth Ejiogu','ABU','ruth.ej@cmtrading.com','2026-03-26','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Eru Jerry Ezekiel ','GMT','eru.j@cmtrading.com','2026-03-26','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Abdulmalik Ademola Abdulrazak ','GMT','abdulrazak.ab@cmtrading.com','2026-03-26','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Abuh Ojodale ','GMT','abuh.oj@cmtrading.com','2025-11-10','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joelle Kamal','GMT','joelle.k@cmtrading.com','2025-09-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jose Damoury','GMT','jose.d@cmtrading.com','2025-11-03','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Anas Ghazo','GMT','anas.g@cmtrading.com','2025-11-15','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Samir Baydoun','GMT','samir.b@cmtrading.com','2025-07-28','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Taghreed Jaafar','GMT','taghreed.j@cmtrading.com','2025-03-25','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Elaine Daher','GMT','elaine.d@cmtrading.com','2025-08-05','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Eliana Bou Khalil','GMT','eliana.k@cmtrading.com','2025-09-08','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Gerard Chaaya','GMT','gerard.c@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Marcelino Ouba','GMT','marcelino.o@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mandy saliba','GMT','mandy.sa@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Yves barhouche','GMT','yves.b@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('wissam sayfe ','GMT','wissam.s@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Georges El Khoury','GMT','georges.ek@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Ahmad khatib','GMT','ahmad.k@cmtrading.com','2026-01-07','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jad Alnachef','GMT','jad.al@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Monica Milad Hage','GMT','monica.m@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Tamer Yehya','GMT','tamer.y@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Elia Hakme','GMT','elia.h@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Charbel Bou Younes','GMT','charbel.b@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Jenny Bou Chahine','GMT','jenny.b@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Serena  Seklawy','GMT','serena.s@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Joseph Jarrouj','GMT','joseph.j@cmtrading.com','2026-02-16','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mido Awad','GMT','mido.a@cmtrading.com','2026-03-02','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Rabih Stephanos','GMT','rabih.s@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Henry El Khoury','GMT','henry.k@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Laurent Khalife','GMT','laurent.k@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Andy Daibess','GMT','andy.d@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Mia Saad','GMT','mia.s@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Maria Awad','GMT','maria.a@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Raquelle Boustani','GMT','raquelle.b@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Elia Khoury','GMT','elia.k@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Tony Semaan','GMT','tony.s@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Danny Boulous','GMT','danny.b@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Stephanie Abboud','GMT','stephanie.a@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;
INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group) VALUES ('Anthony Boulous','GMT','anthony.b@cmtrading.com','2026-03-18','NET') ON CONFLICT (email) DO NOTHING;

-- STEP 3: Match to CRM
UPDATE agents_master am
SET crm_user_id = cu.id
FROM crm_users cu
WHERE LOWER(TRIM(am.email)) = LOWER(TRIM(cu.email))
  AND am.crm_user_id IS NULL;

-- HELPER FUNCTIONS

CREATE OR REPLACE FUNCTION _get_net_target(p_tenure int, p_office text, p_agent text)
RETURNS INTEGER LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF LOWER(TRIM(p_office)) = 'general'
       AND NOT (LOWER(TRIM(p_agent)) LIKE 'noel%' OR LOWER(TRIM(p_agent)) LIKE 'eyad%') THEN
        RETURN NULL;
    END IF;
    RETURN CASE
        WHEN p_tenure <= 1 THEN 0
        WHEN p_tenure = 2  THEN 10000
        WHEN p_tenure = 3  THEN 20000
        WHEN p_tenure = 4  THEN 30000
        WHEN p_tenure = 5  THEN 40000
        WHEN p_tenure = 6  THEN 55000
        WHEN p_tenure = 7  THEN 75000
        WHEN p_tenure = 8  THEN 85000
        ELSE 100000
    END;
END;
$$;

CREATE OR REPLACE FUNCTION _get_ftd100_target(p_tenure int)
RETURNS INTEGER LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN CASE
        WHEN p_tenure = 0  THEN 0
        WHEN p_tenure = 1  THEN 5
        WHEN p_tenure = 2  THEN 10
        WHEN p_tenure = 3  THEN 15
        WHEN p_tenure = 4  THEN 20
        WHEN p_tenure = 5  THEN 25
        WHEN p_tenure = 6  THEN 30
        ELSE 35
    END;
END;
$$;

-- STEP 5: backfill_new_agent

CREATE OR REPLACE FUNCTION backfill_new_agent(p_email TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    a        agents_master%ROWTYPE;
    r_month  DATE;
    t_months INTEGER;
    net_tgt  INTEGER;
    ftd_tgt  INTEGER;
BEGIN
    SELECT * INTO a FROM agents_master
    WHERE LOWER(TRIM(email)) = LOWER(TRIM(p_email));
    IF NOT FOUND THEN
        RAISE NOTICE 'Agent % not found', p_email; RETURN;
    END IF;
    IF a.email IS NULL THEN
        RAISE NOTICE 'Agent % has no email, skipping', a.agent_name; RETURN;
    END IF;
    IF a.start_date IS NULL OR a.start_date > CURRENT_DATE THEN
        RAISE NOTICE 'Agent % has NULL or future start_date, skipping', a.agent_name; RETURN;
    END IF;

    -- Attempt CRM match
    UPDATE agents_master SET crm_user_id = cu.id
    FROM crm_users cu
    WHERE LOWER(TRIM(agents_master.email)) = LOWER(TRIM(cu.email))
      AND agents_master.id = a.id
      AND agents_master.crm_user_id IS NULL;

    SELECT * INTO a FROM agents_master WHERE id = a.id;

    r_month := DATE_TRUNC('month', a.start_date)::date;
    WHILE r_month <= DATE_TRUNC('month', CURRENT_DATE)::date LOOP
        t_months := (
            EXTRACT(YEAR  FROM age(r_month, a.start_date)) * 12 +
            EXTRACT(MONTH FROM age(r_month, a.start_date))
        )::integer;

        net_tgt := _get_net_target(t_months, a.office_name, a.agent_name);
        ftd_tgt := _get_ftd100_target(t_months);

        INSERT INTO agent_targets_history (
            agent_name, office_name, email, start_date, target_group,
            crm_user_id, report_month, tenure_months,
            monthly_net_target, monthly_ftd100_target
        ) VALUES (
            a.agent_name, a.office_name, a.email, a.start_date, a.target_group,
            a.crm_user_id, r_month, t_months, net_tgt, ftd_tgt
        ) ON CONFLICT (email, report_month) DO NOTHING;

        r_month := (r_month + INTERVAL '1 month')::date;
    END LOOP;
    RAISE NOTICE 'Backfill complete for %', a.agent_name;
END;
$$;

-- STEP 6: append_monthly_snapshot

CREATE OR REPLACE FUNCTION append_monthly_snapshot()
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    a        agents_master%ROWTYPE;
    r_month  DATE;
    t_months INTEGER;
    net_tgt  INTEGER;
    ftd_tgt  INTEGER;
BEGIN
    r_month := DATE_TRUNC('month', CURRENT_DATE)::date;

    -- Re-attempt CRM matching
    UPDATE agents_master am SET crm_user_id = cu.id
    FROM crm_users cu
    WHERE LOWER(TRIM(am.email)) = LOWER(TRIM(cu.email))
      AND am.crm_user_id IS NULL;

    -- Sync newly matched crm_user_id into history
    UPDATE agent_targets_history ath SET crm_user_id = am.crm_user_id
    FROM agents_master am
    WHERE LOWER(TRIM(ath.email)) = LOWER(TRIM(am.email))
      AND ath.crm_user_id IS NULL
      AND am.crm_user_id IS NOT NULL;

    FOR a IN SELECT * FROM agents_master WHERE is_active = TRUE LOOP
        CONTINUE WHEN a.email IS NULL;
        CONTINUE WHEN a.start_date IS NULL OR a.start_date > CURRENT_DATE;

        t_months := (
            EXTRACT(YEAR  FROM age(r_month, a.start_date)) * 12 +
            EXTRACT(MONTH FROM age(r_month, a.start_date))
        )::integer;

        net_tgt := _get_net_target(t_months, a.office_name, a.agent_name);
        ftd_tgt := _get_ftd100_target(t_months);

        INSERT INTO agent_targets_history (
            agent_name, office_name, email, start_date, target_group,
            crm_user_id, report_month, tenure_months,
            monthly_net_target, monthly_ftd100_target
        ) VALUES (
            a.agent_name, a.office_name, a.email, a.start_date, a.target_group,
            a.crm_user_id, r_month, t_months, net_tgt, ftd_tgt
        ) ON CONFLICT (email, report_month) DO NOTHING;
    END LOOP;
    RAISE NOTICE 'Monthly snapshot complete for %', r_month;
END;
$$;

-- STEP 4: Backfill all historical rows

DO $$
DECLARE
    a agents_master%ROWTYPE;
BEGIN
    FOR a IN SELECT * FROM agents_master WHERE email IS NOT NULL LOOP
        PERFORM backfill_new_agent(a.email);
    END LOOP;
END $$;

-- Sanity check: Karim K (FTD100, started 2023-12-01) — verify tenure and targets
SELECT agent_name, report_month, tenure_months, monthly_net_target, monthly_ftd100_target, crm_user_id
FROM agent_targets_history
WHERE email = 'karim.k@cmtrading.com'
ORDER BY report_month;

-- STEP 7: pg_cron

SELECT cron.schedule('monthly_targets_snapshot','1 0 1 * *',$$SELECT append_monthly_snapshot();$$);

-- STEP 8: Views

CREATE OR REPLACE VIEW agent_targets_view AS
SELECT agent_name, office_name, email, target_group, crm_user_id,
       report_month, tenure_months, monthly_net_target, monthly_ftd100_target
FROM agent_targets_history
ORDER BY report_month DESC, target_group, agent_name;

CREATE OR REPLACE VIEW agents_unmatched AS
SELECT agent_name, email, target_group, start_date
FROM agents_master
WHERE crm_user_id IS NULL
ORDER BY target_group, agent_name;

-- CRM match report
SELECT 'Matched: ' || COUNT(*) FROM agents_master WHERE crm_user_id IS NOT NULL
UNION ALL
SELECT 'Unmatched: ' || COUNT(*) FROM agents_master WHERE crm_user_id IS NULL;

SELECT agent_name, email FROM agents_master WHERE crm_user_id IS NULL ORDER BY agent_name;

-- Done. To add a new agent:
--   INSERT INTO agents_master (agent_name,office_name,email,start_date,target_group)
--   VALUES ('Name','Office','email@cmtrading.com','2026-05-01','NET');
--   SELECT backfill_new_agent('email@cmtrading.com');
