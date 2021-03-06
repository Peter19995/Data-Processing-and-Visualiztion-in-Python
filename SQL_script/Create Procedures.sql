USE [CO2DB]
GO
/****** Object:  StoredProcedure [dbo].[GetCO2ByDifferentSourcesPerCountry]     ******/

create procedure [dbo].[GetCO2ByDifferentSourcesPerCountry] (@code int, @Reference varchar(max), @countryName varchar(max)) as
begin
	if(@code = 0)
	begin
		select * from vwGasEmmissionCountryStats where Gas_ID = 1
	end
	if(@code = 1)
	begin
		select * from vwGasEmmissionCountryStats where Country_ID = @Reference
	end
	if(@code = 2)
	begin
		select * from vwGasEmmissionCountryStats where Gas_ID = @Reference
	end
	if(@code = 3)
	begin
		select Sources_Name,Amount,Year,Country_Name,Gas_Name  from vwSourcesofCO2PerCountry 
	end
	if(@code = 4)
	begin
		select *  from vwGasEmmissionCountryStats where Country_Name = @countryName and Gas_ID =@Reference 
	end
end

GO
/****** Object:  StoredProcedure [dbo].[GetCO2ByDifferentSourcesPerRegion]     ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[GetCO2ByDifferentSourcesPerRegion] (@code int, @Reference varchar(max)) as
begin
	if(@code = 0)
	begin
		select * from vwSourcesofCO2PerRegion
	end
	if(@code = 1)
	begin
		select * from vwSourcesofCO2PerRegion where Continents_Regions_ID = @Reference
	end
	if(@code = 2)
	begin
		select * from vwSourcesofCO2PerRegion where Source_ID = @Reference
	end
end
GO
/****** Object:  StoredProcedure [dbo].[GetCountryGreenHouseGases]     ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[GetCountryGreenHouseGases] (@code int, @Reference varchar(max)) as
begin
	if(@code = 0)
	begin
		select * from vwCountryGreenHouseGases
	end
	if(@code = 1)
	begin
		select * from vwCountryGreenHouseGases where Country_ID = @Reference
	end
	if(@code = 2)
	begin
		select * from vwCountryGreenHouseGases where Gas_ID = @Reference
	end
end
GO
/****** Object:  StoredProcedure [dbo].[GetCountryStats]     ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[GetCountryStats] (@code int, @Reference varchar(max)) as
begin
	if(@code = 0)
	begin
		select * from vwCountryStats
	end
	if(@code = 1)
	begin
		select * from vwCountryStats  where Country_ID = @Reference
	end
	if(@code = 2)
	begin
		select top 3 Gas_Name from Gases  
	end
end

GO
/****** Object:  StoredProcedure [dbo].[GetRegionGreenHouseGases]     ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[GetRegionGreenHouseGases] (@code int, @Reference varchar(max)) as
begin
	if(@code = 0)
	begin
		select * from vwRegionGreenHouseGases
	end
	if(@code = 1)
	begin
		select * from vwRegionGreenHouseGases  where Continents_Regions_ID = @Reference
	end
	if(@code = 2)
	begin
		select * from vwRegionGreenHouseGases where Gas_ID = @Reference
	end
end
GO
/****** Object:  StoredProcedure [dbo].[GetRegionStats]     ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create procedure [dbo].[GetRegionStats] (@code int, @Reference varchar(max)) as
begin
	if(@code = 0)
	begin
		select * from vwRegionStats
	end
	if(@code = 1)
	begin
		select * from vwRegionStats  where Region_stats_ID = @Reference
	end

	if(@code = 3)
	begin
		select sum(Amount) as Amount_Emitted,[Year],Gas_Name from vwCountryGreenHouseGases group by Year,Gas_Name
	end
	
end
GO
/****** Object:  StoredProcedure [dbo].[GetWidgetsData]     ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create procedure [dbo].[GetWidgetsData] (@Code int, @Reference nvarchar(max)) as 
begin 
	if(@code = 0)
	begin
		select top 1 max(Year) as max_year from vwCountryStats
	end
	if(@code = 1)
	begin
		select top 1 min(Year) as min_year from vwCountryStats
	end
	if(@code = 2)
	begin
		select Distinct top 3  Gas_Name,Gas_ID from Gases  
	end
	if(@code = 3)
	begin
		select  Distinct  Sources_Name,Source_ID  from CO2_SOURCES  
	end
	if(@code = 4)
	begin
		select  Distinct  Country_Name  from Countries  
	end
end 
GO
