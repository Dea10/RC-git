
-- Function: rc.get_events_2_transplace()

-- DROP FUNCTION rc.get_events_2_transplace();

CREATE OR REPLACE FUNCTION rc.get_events_2_transplace()
  RETURNS TABLE(shipmentNumber character varying, serviceProviderOrderId character varying, stopName character varying, stopSequence integer, stopType character varying, eventType character varying, eventMessage character varying, eventDateTime character varying, localDateTime character varying) AS
$BODY$
DECLARE
	_user_timezone text;
	minute_interval text;
BEGIN
	--> Identificar Usuario

      select (value ->> 'minute_interval')::text
      into minute_interval
      from rc.GLOBAL_CONFIG where CONFIG_ID = 'transplace_events';

      RAISE NOTICE 'minute_interval : (%)', minute_interval;

      RETURN QUERY
            SELECT 
                  COALESCE(ST.shipment::character varying,'') as shipmentNumber, 
                  COALESCE(ST.shipment::character varying,'') AS serviceProviderOrderId, 
                  COALESCE(L.LOCATION_NAME, '')::character varying AS stopName,  
                  CASE 
                        WHEN ET.EVENT_TYPE_NAME = 'ENTRANCE' THEN 1
                        WHEN ET.EVENT_TYPE_NAME = 'END_LOAD' THEN 2
                        WHEN ET.EVENT_TYPE_NAME = 'EXIT' THEN 3
                        ELSE 99
                  END AS stopSequence, 
                  'PICKUP'::character varying AS stopType,
                  CASE 
                        WHEN ET.EVENT_TYPE_NAME = 'ENTRANCE' THEN 'STOP_ARRIVAL'::character varying
                        WHEN ET.EVENT_TYPE_NAME = 'END_LOAD' THEN 'END_LOAD'::character varying
                        WHEN ET.EVENT_TYPE_NAME = 'EXIT' THEN 'STOP_DEPARTURE'::character varying
                        ELSE ET.EVENT_TYPE_NAME::character varying
                  END AS eventType,
                  ''::character varying AS eventMessage,
                  UPPER(to_char(TRD.DATE, 'YYYY-MM-DDtHH24:MI:SS'))::character varying AS eventDateTime, 
                  UPPER(to_char(NOW() , 'YYYY-MM-DDtHH24:MI:SS'))::character varying AS localDateTime 
            FROM RC.SUPPLY_CHAIN_COMPANY SCC 
                  INNER JOIN RC.SUPPLY_CHAIN SC  ON SC.SUPPLY_CHAIN_ID = SCC.SUPPLY_CHAIN_ID
                  INNER JOIN RC.SEGMENT      SE  ON SE.SUPPLY_CHAIN_ID = SC.SUPPLY_CHAIN_ID
                  LEFT JOIN RC.STOP          ST  ON ST.STOP_ID = SE.STOP_ID_SOURCE /* EVENT SOURCE */
                  LEFT JOIN RC.location      L   ON L.LOCATION_ID = ST.LOCATION_ID     
                  LEFT JOIN RC.TRACK         TR  ON TR.STOP_ID = ST.STOP_ID
                  LEFT JOIN RC.TRACK_DETAIL  TRD ON TRD.TRACK_ID = TR.TRACK_ID 
                  LEFT JOIN RC.EVENT_TYPE    ET  ON ET.EVENT_TYPE_ID = TRD.EVENT_TYPE_ID
           WHERE scc.COMPANY_ID = 61877
                  AND TRD.DATE IS NOT NULL 
                  AND SCC."type" = 'CUSTOMER'
                  AND TRD.DATE >= (now() - (minute_interval || ' minutes')::interval) and TRD.DATE <= now()
                  AND ET.EVENT_TYPE_NAME IN ('ENTRANE','EXIT', 'END_LOAD') 

   UNION    /* EVENT TARGET */
            SELECT 
                  COALESCE(ST.shipment::character varying,'') as shipmentNumber, 
                  COALESCE(ST.shipment::character varying,'') AS serviceProviderOrderId, 
                  COALESCE(L.LOCATION_NAME, '')::character varying AS stopName,  
                  CASE 
                        WHEN ET.EVENT_TYPE_NAME = 'ENTRANCE' THEN 4
                        WHEN ET.EVENT_TYPE_NAME = 'END_UNLOAD' THEN 5
                        WHEN ET.EVENT_TYPE_NAME = 'EXIT' THEN 6
                        ELSE 99
                  END AS stopSequence, 
                  'DROPOFF'::character varying AS stopType,
                  CASE 
                        WHEN ET.EVENT_TYPE_NAME = 'ENTRANCE' THEN 'STOP_ARRIVAL'::character varying
                        WHEN ET.EVENT_TYPE_NAME = 'END_UNLOAD' THEN 'END_UNLOAD'::character varying
                        WHEN ET.EVENT_TYPE_NAME = 'EXIT' THEN 'STOP_DEPARTURE'::character varying
                        ELSE ET.EVENT_TYPE_NAME::character varying
                  END AS eventType,
                  ''::character varying AS eventMessage,
                  UPPER(to_char(TRD.DATE, 'YYYY-MM-DDtHH24:MI:SS'))::character varying AS eventDateTime, 
                  UPPER(to_char(NOW() , 'YYYY-MM-DDtHH24:MI:SS'))::character varying AS localDateTime 
            FROM RC.SUPPLY_CHAIN_COMPANY SCC 
                  INNER JOIN RC.SUPPLY_CHAIN SC  ON SC.SUPPLY_CHAIN_ID = SCC.SUPPLY_CHAIN_ID
                  INNER JOIN RC.SEGMENT      SE  ON SE.SUPPLY_CHAIN_ID = SC.SUPPLY_CHAIN_ID
                  LEFT JOIN RC.STOP          ST  ON ST.STOP_ID = SE.STOP_ID_DESTINY
                  LEFT JOIN RC.location      L   ON L.LOCATION_ID = ST.LOCATION_ID     
                  LEFT JOIN RC.TRACK         TR  ON TR.STOP_ID = ST.STOP_ID
                  LEFT JOIN RC.TRACK_DETAIL  TRD ON TRD.TRACK_ID = TR.TRACK_ID 
                  LEFT JOIN RC.EVENT_TYPE    ET  ON ET.EVENT_TYPE_ID = TRD.EVENT_TYPE_ID
           WHERE scc.COMPANY_ID = 61877
                  AND TRD.DATE IS NOT NULL 
                  AND SCC.type = 'CUSTOMER'
--                  AND TRD.DATE >= (now() - (minute_interval || ' minutes')::interval) and TRD.DATE <= now()
                  AND ET.EVENT_TYPE_NAME IN ('ENTRANE','EXIT', 'END_UNLOAD') 
     ORDER BY shipmentNumber, stopType DESC, stopSequence;
      

END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION rc.get_events_2_transplace()
  OWNER TO postgres;
