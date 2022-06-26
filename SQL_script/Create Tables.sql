USE CO2DB
go
/****** Object:  Table CO2_Source_Emitters_Per_Country     ******/
CREATE TABLE CO2_Source_Emitters_Per_Country(
	Source_Emitter_ID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	Country_ID int NULL,
	Gas_ID int NULL,
	Source_ID int NULL,
	Year int NULL,
	Amount float NULL,
	Source_Per_Capital float NULL,
	Cumulative float NULL,
	share_global float NULL,
	share_global_cumulative float NULL)
GO

/****** Object:  Table Countries     ******/
CREATE TABLE Countries(
	Country_ID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	iso_code varchar(50) NULL,
	Country_Name varchar(100) NULL)
GO

/****** Object:  Table CO2_SOURCES     ******/
CREATE TABLE CO2_SOURCES(
	Source_ID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	Sources_Name varchar(max) NULL)
GO


/****** Object:  Table Gases     ******/
CREATE TABLE Gases(
	Gas_ID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	Gas_Name varchar(max) NULL)

GO

/****** Object:  Table Continents_and_Regions     ******/
CREATE TABLE Continents_and_Regions(
	Continents_Regions_ID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	[Name] varchar(max) NULL)
GO

/****** Object:  Table CO2_Source_Emitters_Per_Region     ******/
CREATE TABLE CO2_Source_Emitters_Per_Region(
	Source_Emitter_ID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	Continents_Regions_ID int foreign key references Continents_and_Regions (Continents_Regions_ID) NULL,
	Gas_ID int  NULL,
	Source_ID int NULL,
	Year int NULL,
	Amount float NULL,
	Source_Per_Capital float NULL,
	Cumulative float NULL,
	share_global float NULL,
	share_global_cumulative float NULL)
GO

/****** Object:  Table Country_Gas_Emissions     ******/
CREATE TABLE Country_Gas_Emissions(
	Emissions_ID int IDENTITY(1,1) NOT NULL,
	Country_ID int foreign key references Countries(Country_ID) NULL,
	Gas_ID int foreign key references Gases(Gas_ID) NULL,
	Year int NULL,
	Amount float NULL,
	Gas_per_capita float NULL,
	Trade float NULL,
	Growth_prct float NULL,
	Growth_abs float NULL,
	Per_unit_energy float NULL,
	Cumulative float NULL,
	Trade_share float NULL,
	Share_global float NULL,
	Share_global_cumulative float NULL,
	Total_ghg float NULL,
	Ghg_per_capita float NULL,
	Total_ghg_excluding_lucf float NULL)
GO

/****** Object:  Table Country_population_gdp     ******/
CREATE TABLE Country_population_gdp(
	Country_stats_ID int IDENTITY(1,1) NOT NULL,
	Country_ID int foreign key references Countries(Country_ID) NULL,
	Population int NULL,
	Year int NULL,
	primary_energy_consumption varchar(max) NULL,
	Energy_per_capita float NULL,
	Energy_per_gdp float NULL,
	GDP float)
GO

/****** Object:  Table Region_Gas_Emissions     ******/
CREATE TABLE Region_Gas_Emissions(
	Emissions_ID int IDENTITY(1,1) NOT NULL,
	Continents_Regions_ID int foreign key references Continents_and_Regions (Continents_Regions_ID),
	Gas_ID int NULL foreign key references Gases(Gas_ID),
	Year int NULL,
	Amount float NULL,
	Gas_per_capita float NULL,
	Trade float NULL,
	Growth_prct float NULL,
	Growth_abs float NULL,
	Per_unit_energy float NULL,
	Cumulative float NULL,
	Trade_share float NULL,
	Share_global float NULL,
	Share_global_cumulative float NULL,
	Total_ghg float NULL,
	Ghg_per_capita float NULL,
	Total_ghg_excluding_lucf float NULL)
GO

/****** Object:  Table Region_population_gdp     ******/
CREATE TABLE Region_population_gdp(
	Region_stats_ID int IDENTITY(1,1) NOT NULL,
	Continents_Regions_ID int foreign key references Continents_and_Regions (Continents_Regions_ID),
	Population int NULL,
	YEAR int NULL,
	primary_energy_consumption varchar(max) NULL,
	Energy_per_capita float NULL,
	Energy_per_gdp float NULL)
GO
