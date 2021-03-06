USE [CO2DB]
GO
/****** Object:  View [dbo].[vwCountryGreenHouseGases]    ******/
CREATE VIEW [dbo].[vwCountryGreenHouseGases]
AS
SELECT        dbo.Country_Gas_Emissions.*, dbo.Countries.iso_code, dbo.Countries.Country_Name, dbo.Gases.Gas_Name
FROM            dbo.Country_Gas_Emissions INNER JOIN
                         dbo.Countries ON dbo.Country_Gas_Emissions.Country_ID = dbo.Countries.Country_ID INNER JOIN
                         dbo.Gases ON dbo.Country_Gas_Emissions.Gas_ID = dbo.Gases.Gas_ID
GO

/****** Object:  View [dbo].[vwCountryStats]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vwCountryStats]
AS
SELECT        dbo.Countries.*, dbo.Country_population_gdp.Population, dbo.Country_population_gdp.Year, dbo.Country_population_gdp.primary_energy_consumption, dbo.Country_population_gdp.Energy_per_capita, 
                         dbo.Country_population_gdp.Energy_per_gdp, dbo.Country_population_gdp.GDP
FROM            dbo.Country_population_gdp INNER JOIN
                         dbo.Countries ON dbo.Country_population_gdp.Country_ID = dbo.Countries.Country_ID
GO

/****** Object:  View [dbo].[vwGasEmmissionCountryStats]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vwGasEmmissionCountryStats]
AS
SELECT        dbo.Countries.*, dbo.Country_population_gdp.Population, dbo.Country_population_gdp.Year, dbo.Country_population_gdp.primary_energy_consumption, dbo.Country_population_gdp.Energy_per_capita, 
                         dbo.Country_population_gdp.Energy_per_gdp, dbo.Country_Gas_Emissions.Gas_ID, dbo.Country_Gas_Emissions.Year AS Expr1, dbo.Country_Gas_Emissions.Amount, dbo.Country_Gas_Emissions.Gas_per_capita, 
                         dbo.Country_Gas_Emissions.Trade, dbo.Country_Gas_Emissions.Growth_prct, dbo.Country_Gas_Emissions.Growth_abs, dbo.Country_Gas_Emissions.Per_unit_energy, dbo.Country_Gas_Emissions.Cumulative, 
                         dbo.Country_Gas_Emissions.Share_global, dbo.Country_Gas_Emissions.Trade_share, dbo.Country_Gas_Emissions.Share_global_cumulative, dbo.Country_Gas_Emissions.Total_ghg, 
                         dbo.Country_Gas_Emissions.Ghg_per_capita, dbo.Country_Gas_Emissions.Total_ghg_excluding_lucf, dbo.Gases.Gas_Name
FROM            dbo.Countries INNER JOIN
                         dbo.Country_Gas_Emissions ON dbo.Countries.Country_ID = dbo.Country_Gas_Emissions.Country_ID INNER JOIN
                         dbo.Gases ON dbo.Country_Gas_Emissions.Gas_ID = dbo.Gases.Gas_ID INNER JOIN
                         dbo.Country_population_gdp ON dbo.Countries.Country_ID = dbo.Country_population_gdp.Country_ID
GO
/****** Object:  View [dbo].[vwRegionGreenHouseGases]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vwRegionGreenHouseGases]
AS
SELECT        dbo.Region_Gas_Emissions.*, dbo.Continents_and_Regions.Name, dbo.Gases.Gas_Name
FROM            dbo.Region_Gas_Emissions INNER JOIN
                         dbo.Continents_and_Regions ON dbo.Region_Gas_Emissions.Continents_Regions_ID = dbo.Continents_and_Regions.Continents_Regions_ID INNER JOIN
                         dbo.Gases ON dbo.Region_Gas_Emissions.Gas_ID = dbo.Gases.Gas_ID
GO
/****** Object:  View [dbo].[vwRegionStats]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vwRegionStats]
AS
SELECT        dbo.Region_population_gdp.*, dbo.Continents_and_Regions.Name
FROM            dbo.Region_population_gdp INNER JOIN
                         dbo.Continents_and_Regions ON dbo.Region_population_gdp.Continents_Regions_ID = dbo.Continents_and_Regions.Continents_Regions_ID
GO
/****** Object:  View [dbo].[vwSourcesofCO2PerCountry]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[vwSourcesofCO2PerCountry] as
SELECT        dbo.CO2_Source_Emitters_Per_Country.*, dbo.Countries.iso_code, dbo.Countries.Country_Name, dbo.Gases.Gas_Name, dbo.CO2_SOURCES.Sources_Name
FROM            dbo.CO2_Source_Emitters_Per_Country INNER JOIN
                         dbo.Countries ON dbo.CO2_Source_Emitters_Per_Country.Country_ID = dbo.Countries.Country_ID INNER JOIN
                         dbo.Gases ON dbo.CO2_Source_Emitters_Per_Country.Gas_ID = dbo.Gases.Gas_ID INNER JOIN
                         dbo.CO2_SOURCES ON dbo.CO2_Source_Emitters_Per_Country.Source_ID = dbo.CO2_SOURCES.Source_ID
GO
/****** Object:  View [dbo].[vwSourcesofCO2PerRegion]    ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[vwSourcesofCO2PerRegion]
AS
SELECT        dbo.Continents_and_Regions.Name, dbo.CO2_SOURCES.Sources_Name, dbo.Gases.Gas_Name, dbo.CO2_Source_Emitters_Per_Region.*
FROM            dbo.CO2_Source_Emitters_Per_Region INNER JOIN
                         dbo.CO2_SOURCES ON dbo.CO2_Source_Emitters_Per_Region.Source_ID = dbo.CO2_SOURCES.Source_ID INNER JOIN
                         dbo.Continents_and_Regions ON dbo.CO2_Source_Emitters_Per_Region.Continents_Regions_ID = dbo.Continents_and_Regions.Continents_Regions_ID INNER JOIN
                         dbo.Gases ON dbo.CO2_Source_Emitters_Per_Region.Gas_ID = dbo.Gases.Gas_ID
GO

