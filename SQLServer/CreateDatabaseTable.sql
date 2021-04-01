CREATE DATABASE PD_DESAFIO_DE;

USE PD_DESAFIO_DE;

CREATE TABLE tb_universities
(
	Id				INT				NOT NULL UNIQUE,
	Name			VARCHAR(100)		NOT NULL,
	CONSTRAINT PK_tb_universities PRIMARY KEY (Id)
);

CREATE TABLE tb_courses
(
	Id				INT				NOT NULL UNIQUE,
	Name			VARCHAR(200)		NOT NULL,
	CONSTRAINT PK_tb_courses PRIMARY KEY (Id)
);

CREATE TABLE tb_subjects
(
	Id				INT				NOT NULL UNIQUE,
	Name			VARCHAR(200)		NOT NULL,
	CONSTRAINT PK_tb_subjects PRIMARY KEY (Id)
);

CREATE TABLE tb_students
(
	Id				VARCHAR(100)	NOT NULL UNIQUE,
	RegisteredDate	DATETIME		NOT NULL,
	State			VARCHAR(100)		NULL,
	City			VARCHAR(100)		NULL,
	UniversityId	INT				NULL,
	CourseId		INT				NULL,
	SignupSource	VARCHAR(100)		NULL,
	CONSTRAINT PK_tb_students PRIMARY KEY (Id),
	CONSTRAINT FK_tb_students_tb_universities	FOREIGN KEY (UniversityId)
	REFERENCES tb_universities(Id),
	CONSTRAINT FK_tb_students_tb_courses	FOREIGN KEY (CourseId)
	REFERENCES tb_courses(Id)
);

CREATE TABLE tb_sessions
(
	StudentId				VARCHAR(100)	NOT NULL,
	SessionStartTime		DATETIME		NOT NULL,
	StudentClient			VARCHAR(100)		NULL,
	CONSTRAINT FK_tb_sessions_tb_students	FOREIGN KEY (StudentId)
	REFERENCES tb_students(Id)
);

CREATE TABLE tb_subscriptions
(
	StudentId				VARCHAR(100)	NOT NULL,
	PaymentDate				DATETIME		NOT NULL,
	PlanType				VARCHAR(100)		NULL,
	CONSTRAINT FK_tb_subscriptions_tb_students	 FOREIGN KEY (StudentId)
	REFERENCES tb_students(Id)
);


CREATE TABLE tb_student_follow_subject
(
	StudentId		VARCHAR(100)		NOT NULL UNIQUE,
	SubjectId		INT					NOT NULL,
	FoollowDate		DATETIME			NOT NULL,
	CONSTRAINT FK_tb_student_follow_subject_tb_students	FOREIGN KEY (StudentId)
	REFERENCES tb_students(Id),
	CONSTRAINT FK_tb_student_follow_subject_tb_subjects	FOREIGN KEY (SubjectId)
	REFERENCES tb_subjects(Id)
);

CREATE TABLE tb_sessions_students_profile
(
	StudentId		VARCHAR(100)	NOT NULL,
	State			VARCHAR(100)		NULL,
	City			VARCHAR(100)		NULL,
	UniversityId	INT				NULL,
	CourseId		INT				NULL,
	SignupSource	VARCHAR(100)		NULL,
	RegisteredDate	DATE			NULL,
	RegisteredHour	TIME			NULL,
	SessionDate		DATE			NULL,
	SessionHour		TIME			NULL,
	StudentClient	VARCHAR(100)		NULL,
	PaymentDate		DATETIME		NULL,
	PlanType		VARCHAR(100)		NULL,
	ExpirationDate	DATE			NULL,
	ExpirationTime	TIME			NULL,
	CONSTRAINT FK_tb_sessions_students_profile_tb_students	FOREIGN KEY (StudentId)
	REFERENCES tb_students(Id)
);

CREATE INDEX IX_SessionDate ON tb_sessions_students_profile (SessionDate);