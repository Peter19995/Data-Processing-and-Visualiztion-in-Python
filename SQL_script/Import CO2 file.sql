-- incaset the Holdiing table exists, we will drop it
begin try drop table #HoldingTB  end try begin catch end catch
go

-->> create a holding temporar table that will store scv file as it is
begin try  
	create table #HoldingTB (iso_code varchar(max),country varchar(max),year int,co2 float,co2_per_capita float,trade_co2 float,cement_co2 float,cement_co2_per_capita float,coal_co2 float,coal_co2_per_capita float,flaring_co2 float,flaring_co2_per_capita float,gas_co2 float,gas_co2_per_capita float,oil_co2 float,oil_co2_per_capita float,other_industry_co2 float,other_co2_per_capita float,co2_growth_prct float,co2_growth_abs float,co2_per_gdp float,co2_per_unit_energy float,consumption_co2 float,consumption_co2_per_capita float,consumption_co2_per_gdp float,cumulative_co2 float,cumulative_cement_co2 float,cumulative_coal_co2 float,cumulative_flaring_co2 float,cumulative_gas_co2 float,cumulative_oil_co2 float,cumulative_other_co2 float,trade_co2_share float,share_global_co2 float,share_global_cement_co2 float,share_global_coal_co2 float,share_global_flaring_co2 float,share_global_gas_co2 float,share_global_oil_co2 float,share_global_other_co2 float,share_global_cumulative_co2 float,share_global_cumulative_cement_co2 float,share_global_cumulative_coal_co2 float,share_global_cumulative_flaring_co2 float,share_global_cumulative_gas_co2 float,share_global_cumulative_oil_co2 float,share_global_cumulative_other_co2 float,total_ghg float,ghg_per_capita float,total_ghg_excluding_lucf float,ghg_excluding_lucf_per_capita float,methane float,methane_per_capita float,nitrous_oxide float,nitrous_oxide_per_capita float,population float,gdp float,primary_energy_consumption float,energy_per_capita float,energy_per_gdp float)
end try begin catch end catch
---> truncate table to remove any data. 
truncate table #HoldingTB 

-->> reading CSV file
BULK INSERT #HoldingTB
FROM 'C:\data\owid-co2-data.csv'
WITH
(
    --FORMAT = 'CSV', 
	DATAFILETYPE = 'char',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '\n',  
    TABLOCK
)
-->> alter table to add identity column that we will use to loop through the table rows as we extract data 
alter table #HoldingTB add id int identity(1,1) 

-->> afer reading we select top 10
select top 10  * from #HoldingTB 

-->> lets create GASSES
Truncate table Gases
go
INSERT INTO Gases(Gas_Name) VALUES('carbon dioxide (CO2)'),('Methane'),('Nitrous Oxide')


-->> Sources of CO@
Truncate table Gases
go
INSERT INTO CO2_SOURCES(Sources_Name) VALUES('Cement'),('Coal'),('Flaring'),('Gas'),('Oil'),('Other_industry')


-->> let us know create a loop and start extracting data as we insert into our table
Declare @start int = 1, @Strop int 
Declare @iso_code varchar(max),@country varchar(max),@year int,@co2 float,@co2_per_capita float,@trade_co2 float,@cement_co2 float,@cement_co2_per_capita float,@coal_co2 float,@coal_co2_per_capita float,@flaring_co2 float,@flaring_co2_per_capita float,@gas_co2 float,
@gas_co2_per_capita float,@oil_co2 float,@oil_co2_per_capita float,@other_industry_co2 float,@other_co2_per_capita float,@co2_growth_prct float,@co2_growth_abs float,@co2_per_gdp float,@co2_per_unit_energy float,@consumption_co2 float,@consumption_co2_per_capita float,
@consumption_co2_per_gdp float,@cumulative_co2 float,@cumulative_cement_co2 float,@cumulative_coal_co2 float,@cumulative_flaring_co2 float,@cumulative_gas_co2 float,@cumulative_oil_co2 float,@cumulative_other_co2 float,@trade_co2_share float,@share_global_co2 float,
@share_global_cement_co2 float,@share_global_coal_co2 float,@share_global_flaring_co2 float,@share_global_gas_co2 float,@share_global_oil_co2 float,@share_global_other_co2 float,@share_global_cumulative_co2 float,@share_global_cumulative_cement_co2 float,
@share_global_cumulative_coal_co2 float,@share_global_cumulative_flaring_co2 float,@share_global_cumulative_gas_co2 float,@share_global_cumulative_oil_co2 float,@share_global_cumulative_other_co2 float,@total_ghg float,@ghg_per_capita float,@total_ghg_excluding_lucf float,
@ghg_excluding_lucf_per_capita float,@methane float,@methane_per_capita float,@nitrous_oxide float,@nitrous_oxide_per_capita float,@population float,@gdp float,@primary_energy_consumption float,@energy_per_capita float,@energy_per_gdp float

Declare @Country_ID int, @Continents_Regions_ID int , @Gas_ID int,@Source_ID int

-->> set the loop max 
set @Strop = (select count(*) from #HoldingTB)
while(@start <= @Strop)
begin
	-->> extract row data
	select 
	@iso_code= iso_code ,@country= country,@year= [year],@co2=co2 ,@co2_per_capita= co2_per_capita,@trade_co2=trade_co2 ,@cement_co2=cement_co2 ,@cement_co2_per_capita= cement_co2_per_capita,@coal_co2= coal_co2,@coal_co2_per_capita= coal_co2_per_capita,@flaring_co2=flaring_co2 ,@flaring_co2_per_capita= flaring_co2_per_capita,@gas_co2=gas_co2 ,
	@gas_co2_per_capita=gas_co2_per_capita ,@oil_co2=oil_co2 ,@oil_co2_per_capita= oil_co2_per_capita,@other_industry_co2=other_industry_co2 ,@other_co2_per_capita= other_co2_per_capita,@co2_growth_prct= co2_growth_prct,@co2_growth_abs=co2_growth_abs ,@co2_per_gdp=co2_per_gdp ,@co2_per_unit_energy=co2_per_unit_energy ,@consumption_co2=consumption_co2 ,@consumption_co2_per_capita= consumption_co2_per_capita,
	@consumption_co2_per_gdp=consumption_co2_per_gdp ,@cumulative_co2=cumulative_co2 ,@cumulative_cement_co2=cumulative_cement_co2 ,@cumulative_coal_co2=cumulative_coal_co2 ,@cumulative_flaring_co2=cumulative_flaring_co2 ,@cumulative_gas_co2=cumulative_gas_co2 ,@cumulative_oil_co2=cumulative_oil_co2 ,@cumulative_other_co2=cumulative_other_co2 ,@trade_co2_share= trade_co2_share,@share_global_co2=share_global_co2 ,
	@share_global_cement_co2=share_global_cement_co2 ,@share_global_coal_co2=share_global_coal_co2 ,@share_global_flaring_co2= share_global_flaring_co2,@share_global_gas_co2=share_global_gas_co2 ,@share_global_oil_co2= share_global_oil_co2,@share_global_other_co2=share_global_other_co2 ,@share_global_cumulative_co2=share_global_cumulative_co2 ,@share_global_cumulative_cement_co2= share_global_cumulative_cement_co2,
	@share_global_cumulative_coal_co2= share_global_cumulative_coal_co2,@share_global_cumulative_flaring_co2=share_global_cumulative_flaring_co2 ,@share_global_cumulative_gas_co2= share_global_cumulative_gas_co2,@share_global_cumulative_oil_co2= share_global_cumulative_oil_co2,@share_global_cumulative_other_co2= share_global_cumulative_other_co2,@total_ghg= total_ghg,@ghg_per_capita=ghg_per_capita ,@total_ghg_excluding_lucf=total_ghg_excluding_lucf ,
	@ghg_excluding_lucf_per_capita=ghg_excluding_lucf_per_capita ,@methane=methane ,@methane_per_capita=methane_per_capita ,@nitrous_oxide=nitrous_oxide ,@nitrous_oxide_per_capita= nitrous_oxide_per_capita,@population= [population],@gdp= gdp,@primary_energy_consumption= primary_energy_consumption,@energy_per_capita= energy_per_capita,@energy_per_gdp=energy_per_gdp from #HoldingTB where Id = @start
	
	-->> iF THE ROW CONTAINS DATA THAT BELONGS TO A COUNTY 
	if(@iso_code is not null )
	begin
		-->> check if the country exists
		if not exists(select 1 from Countries where iso_code = @iso_code and Country_Name = @country)
		begin
			insert into Countries (iso_code,Country_Name) values (@iso_code,@country) 
		end
		set @Country_ID = (select Country_ID from Countries where iso_code = @iso_code and Country_Name = @country)

		if not exists(select 1 from Country_population_gdp where Country_ID = @Country_ID and [YEAR] = @year )
		begin
			insert into Country_population_gdp (Country_ID,[Population],[Year],primary_energy_consumption,Energy_per_capita,Energy_per_gdp,GDP ) 
			values (@Country_ID,isnull(@Population,0),@year,isnull(@primary_energy_consumption,0),isnull(@Energy_per_capita,0),isnull(@Energy_per_gdp,0),isnull(@gdp,0)) 
		end
		

		-->> GAS EMISSIONS
		-->> log Country Gas Emissions CO2 GAS DATA
		set @Gas_ID = (select top 1  Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from Country_Gas_Emissions  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year)
		begin
			insert into Country_Gas_Emissions ([year],Country_ID ,Gas_ID,Amount,Gas_per_capita,Trade,Growth_prct,Growth_abs ,Per_unit_energy,Cumulative ,Trade_share ,Share_global ,Share_global_cumulative ,Total_ghg ,Ghg_per_capita ,Total_ghg_excluding_lucf)
			values(@year,@Country_ID,@Gas_ID,isnull(@co2,0),isnull(@co2_per_capita,0),isnull(@trade_co2,0),isnull(@co2_growth_prct,0),isnull(@co2_growth_abs,0),isnull(@co2_per_unit_energy,0),isnull(@cumulative_co2,0),isnull(@trade_co2_share,0),isnull(@share_global_co2,0),isnull(@share_global_cumulative_co2,0),isnull(@total_ghg,0),isnull(@ghg_per_capita,0),isnull(@total_ghg_excluding_lucf,0))
		end
		set @Gas_ID  = null
	
		-->> log Country Gas Emissions Methane GAS DATA
		set @Gas_ID = (select top 1  Gas_ID from Gases where Gas_Name = 'Methane')
		if not exists(select 1 from Country_Gas_Emissions  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year)
		begin
			insert into Country_Gas_Emissions ([year],Country_ID ,Gas_ID,Amount,Gas_per_capita,Trade,Growth_prct,Growth_abs ,Per_unit_energy,Cumulative ,Trade_share ,Share_global ,Share_global_cumulative ,Total_ghg ,Ghg_per_capita ,Total_ghg_excluding_lucf)
			values(isnull(@year,0),@Country_ID,@Gas_ID,isnull(@methane,0),isnull(@methane_per_capita,0),0,0,0,0,0,0,0,0,0,0,0)
		end
		set @Gas_ID  = null

		-->> log Country Gas Emissions Methane GAS DATA
		set @Gas_ID = (select top 1  Gas_ID from Gases where Gas_Name = 'Nitrous Oxide')
		if not exists(select 1 from Country_Gas_Emissions  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year)
		begin
			insert into Country_Gas_Emissions ([year],Country_ID ,Gas_ID,Amount,Gas_per_capita,Trade,Growth_prct,Growth_abs ,Per_unit_energy,Cumulative ,Trade_share ,Share_global ,Share_global_cumulative ,Total_ghg ,Ghg_per_capita ,Total_ghg_excluding_lucf)
			values(isnull(@year,0),@Country_ID,@Gas_ID,isnull(@nitrous_oxide,0),isnull(@nitrous_oxide_per_capita,0),0,0,0,0,0,0,0,0,0,0,0)
		end
		set @Gas_ID  = null


		-->> SOURCES OF CO2
		-- 1. cement
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Cement')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Country  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Country (Country_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(@Country_ID,@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@cement_co2,0),Isnull(@cement_co2_per_capita,0),Isnull(@cumulative_cement_co2,0),Isnull(@share_global_cement_co2,0),Isnull(@share_global_cumulative_cement_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 2. Coal
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Coal')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Country  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Country (Country_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(@Country_ID,@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@coal_co2,0),Isnull(@coal_co2_per_capita,0),Isnull(@cumulative_coal_co2,0),Isnull(@share_global_coal_co2,0),Isnull(@share_global_cumulative_coal_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 3. flaring
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'flaring')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Country  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Country (Country_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(@Country_ID,@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@flaring_co2,0),Isnull(@flaring_co2_per_capita,0),Isnull(@cumulative_flaring_co2,0),Isnull(@share_global_flaring_co2,0),Isnull(@share_global_cumulative_flaring_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 4. gas
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Gas')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Country  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Country (Country_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(@Country_ID,@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@gas_co2,0),Isnull(@gas_co2_per_capita,0),Isnull(@cumulative_gas_co2,0),Isnull(@share_global_gas_co2,0),Isnull(@share_global_cumulative_gas_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 5. oil
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Oil')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Country  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Country (Country_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(@Country_ID,@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@oil_co2,0),Isnull(@oil_co2_per_capita,0),Isnull(@cumulative_oil_co2,0),Isnull(@share_global_oil_co2,0),Isnull(@share_global_cumulative_oil_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 6. other_industry
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'other_industry')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Country  where Country_ID = @Country_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Country (Country_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(@Country_ID,@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@other_industry_co2,0),Isnull(@other_co2_per_capita,0),Isnull(@cumulative_other_co2,0),Isnull(@share_global_other_co2,0),Isnull(@share_global_cumulative_other_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null


	end
	

	-->> iF THE ROW CONTAINS DATA THAT BELONGS TO A REGION/CONTINENTS
	if(@iso_code is null )
	begin
		-->> check if the Region does not  exists
		if not exists(select 1 from Continents_and_Regions where [Name] = @country )
		begin
			insert into Continents_and_Regions ([Name]) values (@country) 
		end
		set @Continents_Regions_ID = (select Continents_Regions_ID from Continents_and_Regions where [Name] = @country)


		if not exists(select 1 from Region_population_gdp where @Continents_Regions_ID =@Continents_Regions_ID  and [YEAR] = @year )
		begin
			insert into Region_population_gdp (Continents_Regions_ID,[Population],[Year],primary_energy_consumption,Energy_per_capita,Energy_per_gdp ) 
			values (@Continents_Regions_ID,isnull(@Population,0),@year,isnull(@primary_energy_consumption,0),isnull(@Energy_per_capita,0),isnull(@Energy_per_gdp,0) ) 
		end


		


		-->> GAS EMISSIONS
		-->> log Country Gas Emissions CO2 GAS DATA
		set @Gas_ID = (select top 1  Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from Region_Gas_Emissions  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year )
		begin
			insert into Region_Gas_Emissions ([year],Continents_Regions_ID ,Gas_ID,Amount,Gas_per_capita,Trade,Growth_prct,Growth_abs ,Per_unit_energy,Cumulative ,Trade_share ,Share_global ,Share_global_cumulative ,Total_ghg ,Ghg_per_capita ,Total_ghg_excluding_lucf)
			values(isnull(@year,0),isnull(@Continents_Regions_ID,0),@Gas_ID,isnull(@co2,0),isnull(@co2_per_capita,0),isnull(@trade_co2,0),isnull(@co2_growth_prct,0),isnull(@co2_growth_abs,0),isnull(@co2_per_unit_energy,0),isnull(@cumulative_co2,0),isnull(@trade_co2_share,0),isnull(@share_global_co2,0),isnull(@share_global_cumulative_co2,0),isnull(@total_ghg,0),isnull(@ghg_per_capita,0),isnull(@total_ghg_excluding_lucf,0))
		end
		set @Gas_ID  = null

		set @Gas_ID = (select top 1  Gas_ID from Gases where Gas_Name = 'Methane')
		if not exists(select 1 from Region_Gas_Emissions  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year)
		begin
			insert into Region_Gas_Emissions ([year],Continents_Regions_ID ,Gas_ID,Amount,Gas_per_capita,Trade,Growth_prct,Growth_abs ,Per_unit_energy,Cumulative ,Trade_share ,Share_global ,Share_global_cumulative ,Total_ghg ,Ghg_per_capita ,Total_ghg_excluding_lucf)
			values(isnull(@year,0),isnull(@Continents_Regions_ID,0),@Gas_ID,isnull(@co2,0),isnull(@co2_per_capita,0),isnull(@trade_co2,0),isnull(@co2_growth_prct,0),isnull(@co2_growth_abs,0),isnull(@co2_per_unit_energy,0),isnull(@cumulative_co2,0),isnull(@trade_co2_share,0),isnull(@share_global_co2,0),isnull(@share_global_cumulative_co2,0),isnull(@total_ghg,0),isnull(@ghg_per_capita,0),isnull(@total_ghg_excluding_lucf,0))
		end
		set @Gas_ID  = null

		set @Gas_ID = (select top 1  Gas_ID from Gases where Gas_Name = 'Nitrous Oxide')
		if not exists(select 1 from Region_Gas_Emissions  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year)
		begin
			insert into Region_Gas_Emissions ([year],Continents_Regions_ID ,Gas_ID,Amount,Gas_per_capita,Trade,Growth_prct,Growth_abs ,Per_unit_energy,Cumulative ,Trade_share ,Share_global ,Share_global_cumulative ,Total_ghg ,Ghg_per_capita ,Total_ghg_excluding_lucf)
			values(isnull(@year,0),isnull(@Continents_Regions_ID,0),@Gas_ID,isnull(@co2,0),isnull(@co2_per_capita,0),isnull(@trade_co2,0),isnull(@co2_growth_prct,0),isnull(@co2_growth_abs,0),isnull(@co2_per_unit_energy,0),isnull(@cumulative_co2,0),isnull(@trade_co2_share,0),isnull(@share_global_co2,0),isnull(@share_global_cumulative_co2,0),isnull(@total_ghg,0),isnull(@ghg_per_capita,0),isnull(@total_ghg_excluding_lucf,0))
		end
		set @Gas_ID  = null




		-->> SOURCES OF CO2
		-- 1. cement
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Cement')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Region  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Region (Continents_Regions_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(isnull(@Continents_Regions_ID,0),@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@cement_co2,0),Isnull(@cement_co2_per_capita,0),Isnull(@cumulative_cement_co2,0),Isnull(@share_global_cement_co2,0),Isnull(@share_global_cumulative_cement_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 2. Coal
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Coal')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Region  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Region (Continents_Regions_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(isnull(@Continents_Regions_ID,0),@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@coal_co2,0),Isnull(@coal_co2_per_capita,0),Isnull(@cumulative_coal_co2,0),Isnull(@share_global_coal_co2,0),Isnull(@share_global_cumulative_coal_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 3. flaring
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'flaring')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Region  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Region (Continents_Regions_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(isnull(@Continents_Regions_ID,0),@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@flaring_co2,0),Isnull(@flaring_co2_per_capita,0),Isnull(@cumulative_flaring_co2,0),Isnull(@share_global_flaring_co2,0),Isnull(@share_global_cumulative_flaring_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 4. gas
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Gas')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Region  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Region (Continents_Regions_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(isnull(@Continents_Regions_ID,0),@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@gas_co2,0),Isnull(@gas_co2_per_capita,0),Isnull(@cumulative_gas_co2,0),Isnull(@share_global_gas_co2,0),Isnull(@share_global_cumulative_gas_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 5. oil
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'Oil')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Region  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Region (Continents_Regions_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(isnull(@Continents_Regions_ID,0),@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@oil_co2,0),Isnull(@oil_co2_per_capita,0),Isnull(@cumulative_oil_co2,0),Isnull(@share_global_oil_co2,0),Isnull(@share_global_cumulative_oil_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null

		-- 6. other_industry
		set @Source_ID = (select top 1 Source_ID from CO2_SOURCES where Sources_Name = 'other_industry')
		set @Gas_ID = (select top 1 Gas_ID from Gases where Gas_Name = 'carbon dioxide (CO2)')
		if not exists(select 1 from CO2_Source_Emitters_Per_Region  where Continents_Regions_ID = @Continents_Regions_ID and Gas_ID = @Gas_ID and [Year] = @year and Source_ID = @Source_ID)
		begin
			insert into CO2_Source_Emitters_Per_Region (Continents_Regions_ID,Gas_ID ,Source_ID, [Year], Amount ,Source_Per_Capital ,Cumulative ,share_global ,share_global_cumulative )
			values(isnull(@Continents_Regions_ID,0),@Gas_ID,Isnull(@Source_ID,0),Isnull(@year,0),Isnull(@other_industry_co2,0),Isnull(@other_co2_per_capita,0),Isnull(@cumulative_other_co2,0),Isnull(@share_global_other_co2,0),Isnull(@share_global_cumulative_other_co2,0))
		end
		set @Gas_ID  = null set @Source_ID = null



	end
		

	set @Country_ID = null set @Continents_Regions_ID = null set @Source_ID  = null set @Gas_ID = null
	set @iso_code= null  SET @country= null  SET @year= null  SET @co2= null  SET @co2_per_capita= null  SET @trade_co2= null  SET @cement_co2= null  SET @cement_co2_per_capita= null  SET @coal_co2= null  SET @coal_co2_per_capita= null  SET @flaring_co2= null  SET @flaring_co2_per_capita= null  SET @gas_co2= null   
	SET @gas_co2_per_capita= null  SET @oil_co2= null  SET @oil_co2_per_capita= null  SET @other_industry_co2= null  SET @other_co2_per_capita= null  SET @co2_growth_prct= null  SET @co2_growth_abs= null  SET @co2_per_gdp= null  SET @co2_per_unit_energy= null  SET @consumption_co2= null  SET @consumption_co2_per_capita= null  
	SET @consumption_co2_per_gdp= null  SET @cumulative_co2= null  SET @cumulative_cement_co2= null  SET @cumulative_coal_co2= null  SET @cumulative_flaring_co2= null  SET @cumulative_gas_co2= null  SET @cumulative_oil_co2= null  SET @cumulative_other_co2= null  SET @trade_co2_share= null  SET @share_global_co2= null  
	SET @share_global_cement_co2= null  SET @share_global_coal_co2= null  SET @share_global_flaring_co2= null  SET @share_global_gas_co2= null  SET @share_global_oil_co2= null  SET @share_global_other_co2= null  SET @share_global_cumulative_co2= null  SET @share_global_cumulative_cement_co2= null  
	SET @share_global_cumulative_coal_co2= null  SET @share_global_cumulative_flaring_co2= null  SET @share_global_cumulative_gas_co2= null  SET @share_global_cumulative_oil_co2= null  SET @share_global_cumulative_other_co2= null  SET @total_ghg= null  SET @ghg_per_capita= null  SET @total_ghg_excluding_lucf= null  
	SET @ghg_excluding_lucf_per_capita= null  SET @methane= null  SET @methane_per_capita= null  SET @nitrous_oxide= null  SET @nitrous_oxide_per_capita= null  SET @population= null  SET @gdp= null  SET @primary_energy_consumption= null  SET @energy_per_capita= null  SET @energy_per_gdp = NULL
	set @start +=1
end

