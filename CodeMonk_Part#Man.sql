CREATE DEFINER=`root`@`localhost` PROCEDURE `Cmonk_partition_manager`(in partition_frequency varchar(100), in db_schema varchar(100),in input_table_name varchar(100), in partition_column varchar(100))
BEGIN
	-- Author 	- Code Monk
	-- Version 	- 1.0
        -- Procedure for automated partitioning of table
        -- Inputs : 
		
		-- 1- Partition_frequency	: Options-(Daily,Monthly,Weekly)
		-- 2- db_schema 		: Name of Database schema
		-- 3- input_table_name		: Table Name
		-- 4- partition_column		: Table column
			
        
        -- Limitations:
		-- 1- Only range partitioning is supported.
		-- 2- Partition column type should be Datetime 
	
	-- Variables to set:
	--	no_of_partitions: number of partitions after last partition.
		
	--	Eg: If current date ='2015-11-07' ,frequency ="daily" and no_of_partitions=4
	--	    then it will also create partitions for next coming 4 days from current_date,
	--	    which means last date of partition will be "2015-11-11".
		
	-- Note: For Automatic partitioning, Schedule it using MySQL Event.

		
	
        declare partitions_count int; 
        declare last_partition_value datetime;
        declare partition_query text;
        declare no_of_partitions int;
        declare first_day_of_current_month datetime;
        declare partition_name varchar(256);
        declare partition_start_date date;
        declare partition_last_date date;
        declare partition_names text;
        declare partition_date date;
        declare temp_current_partition_count int;
        
        -- First day of current month. Eg: '2015-11-01'
        set first_day_of_current_month=date(date_sub(now(),interval dayofmonth(now())-1 day));
        set no_of_partitions=2;
        set partition_names=' ';
        
        -- Increase group concat maximum length. 
        set @@group_concat_max_len = 100000;
        
        -- Get last partition detail of input table
        drop temporary table if exists temp_partition_information;
		create temporary table temp_partition_information
		select table_name, PARTITION_NAME,PARTITION_DESCRIPTION
		from information_schema.PARTITIONS 
		where TABLE_SCHEMA=db_schema and table_name=input_table_name
		and PARTITION_DESCRIPTION is not null
		order by PARTITION_DESCRIPTION desc limit 1;
        
        
        set temp_current_partition_count=(select count(*) from temp_partition_information);
        
        -- If currently there is no partitions exists for table
       if  temp_current_partition_count=0
       then
      	   -- Get minimum value of partition column
	    set @temp_query=concat('set @min_value_of_partition=(','select ',partition_column,' from ',db_schema,'.',input_table_name,' order by cast(',partition_column,' as datetime) asc limit 1',')');
			
		select @temp_query;
	   prepare sql_statement from @temp_query; 
	    execute sql_statement; 
	    deallocate prepare sql_statement;
           
           -- if there is no record in table 
            if @min_value_of_partition is null
            then
				 -- set minimum value current system date
				set @min_value_of_partition=current_date;
	        end if;
            
	else	
		-- Get partition descripion of last partition created on table
		set @min_value_of_partition= (select from_days(partition_description) from temp_partition_information);
	end if;
    
	select @min_value_of_partition;
        
        -- Calaculate start and last date of partitions based on the frequency 
        case when partition_frequency='daily' 
        then
		select @min_value_of_partition;
			set partition_start_date=case when temp_current_partition_count>0 
									then 
										date_ADD(date(@min_value_of_partition), interval 1 day)
                                    ELSE date_sub(date(@min_value_of_partition), interval 1 day) END;
                                    
            set partition_last_date=date_add(date(current_date), interval no_of_partitions+1 day);    
	when partition_frequency ='weekly'
        then 
		set partition_start_date=date_sub(date_add(date(@min_value_of_partition), interval 6-(WEEKDAY(date(@min_value_of_partition))) day),interval 7 day);
               set partition_last_date=date_add(date_add(date(current_date), interval 6-(WEEKDAY(date(current_date))) day),interval no_of_partitions*7 day);                
        when partition_frequency='monthly'
	then 
			set partition_start_date=case when temp_current_partition_count>0 
									then
										date_Add(date_sub(date(@min_value_of_partition), interval dayofmonth(@min_value_of_partition)-1 day),interval 1 month)
									else
										date_sub(date(@min_value_of_partition), interval dayofmonth(@min_value_of_partition)-1 day)
                                    end ;
            set partition_last_date=date_add(date(first_day_of_current_month), interval no_of_partitions+1 month);
	end case;
        
	 
		set partition_date=partition_start_date;
		select partition_start_date,partition_last_date,partition_date;
            
        -- Temporary table to hold the dynamically created partition details
		drop temporary table if exists temp_partition_name;
       create temporary table  temp_partition_name
       (
			partition_list varchar(256)
            
       );
        
        -- Loop create partition query for each partition
		loop_to_make_partition_query:REPEAT 
			
            set partition_name=concat('p',year(partition_date),
            if( length(month(partition_date))=1,concat('0',month(partition_date)),month(partition_date)),
            if( length(day(partition_date))=1,concat('0',day(partition_date)),day(partition_date) )
            );
           		-- Prepare partition values and store it to the temp table
			insert into temp_partition_name values( concat('PARTITION ', partition_name,' VALUES LESS THAN (','to_days(','''',partition_date,'''',')',')'));
            
            		-- Calaculate next partition date based on the frequency of partitioning
			case when partition_frequency='daily' 
			then
				set partition_date=date_Add(partition_date, interval 1 day);  
			when partition_frequency ='weekly'
			then 
				set partition_date=date_Add(partition_date, interval 7 day); 
			when partition_frequency='monthly'
			then 
				set partition_date=date_Add(partition_date, interval 1 month);
			end case;
            
            
            -- set partition_date=date_Add(partition_date, interval 1 month);
	-- Prepare partition query till it reach to the last partition date            
        until partition_last_date<=partition_date 
        END REPEAT loop_to_make_partition_query;
           
       --  select * from temp_partition_name;
       -- After collecting all partition details in the temporary table. concat all partition query in comma seperated format
        select group_concat(partition_list) into partition_names from temp_partition_name;
        
        -- Preparing final partition query and store it to the variable "partition_query"
        if  temp_current_partition_count=0
        then
			set partition_query=concat('alter table ',db_schema,'.',input_table_name,' partition by range(to_days(',partition_column,'))',
								'(',partition_names,');');    
		else
			set partition_query=concat('alter table ',db_schema,'.',input_table_name,' add partition(',partition_names,');');
		end if;
		
		select partition_query ;
    select partition_query into @partition_query;
    SELECT * FROM temp_partition_name;
    
    IF(partition_last_date>=partition_date)
    THEN
    		-- Execute query to add new partitions to table
		prepare stmt from @partition_query;
		 execute stmt;
		deallocate prepare stmt;
	END IF;
END