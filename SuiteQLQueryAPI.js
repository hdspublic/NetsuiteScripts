/**
* @NApiVersion 2.1
* @NScriptType Restlet
* @NModuleScope Public
*/

define(function(require) {		
    var log = require('N/log');
    var search = require('N/search');
    var query = require('N/query');
    
    //To Access System notes. the "CORE ADMINISTRATION PERMISSIONS" permission needs to be granted on the role. 
    //along with Audit Trail
    var testQuery = `select top 100 * from SystemNote`;

    var benchSamplesQuery = `
        select 
                bs.id,
                bs.custrecord162 as OpportunityId,     
                BUILTIN.DF(bs.custrecord162) as Opportunity,               
                BUILTIN.DF(bs.custrecord176) as Division, 
                bs.custrecord168 as CustomerId,
                BUILTIN.DF(bs.custrecord168) as Customer, 

                bs.name as BenchSampleName,
                bs.altname as FormulaNumber,
                BUILTIN.DF(bs.custrecord164) as ProductCategory,
                BUILTIN.DF(bs.custrecord174) as Status, 
                BUILTIN.DF(bs.custrecord175) as RNDSubStatus, 
                BUILTIN.DF(bs.custrecord177) as SalesPerson, 
                BUILTIN.DF(bs.custrecord179) as AssignedTo, 
                BUILTIN.DF(bs.custrecord178) as AccountManager, 
                bs.created,
                bs.custrecord1531 as closed,
                bs.custrecord165 as needByDate,
                sn1.date as InProgressDate,
                sn2.date as completedDate,
                ROW_NUMBER() OVER (PARTITION BY bs.id  ORDER BY sn1.date) as rownumber
        from customrecord_benchsamples as bs
        join transaction as t on bs.custrecord162 = t.id
        left join SystemNote as sn1 on BUILTIN.DF(sn1.recordtypeid)  = 'Bench Samples' and sn1.recordid = bs.id and sn1.field = 'CUSTRECORD175' and sn1.newvalue = 'In Progress'
        left join SystemNote as sn2 on BUILTIN.DF(sn2.recordtypeid)  = 'Bench Samples' and sn2.recordid = bs.id and sn2.field = 'CUSTRECORD175' and sn2.newvalue = 'Complete'
    `;

    var opportunityDatesQuery = `
        select 
            sn.recordid, 
            min (t.createddate) as created_date, 
            min(case when sn.field = 'CUSTBODY_QUOTE_A' and sn.newvalue = 'T' then sn.date else null end) as date_a,
            min(case when sn.field = 'CUSTBODY_QUOTE_B' and sn.newvalue = 'T' then sn.date else null end) as date_b,
            min(case when sn.field = 'CUSTBODY_QUOTE_C' and sn.newvalue = 'T' then sn.date else null end) as date_c,
            min(case when sn.field = 'CUSTBODY_QUOTE_D' and sn.newvalue = 'T' then sn.date else null end) as date_d,
            min(case when sn.field = 'CUSTBODY_QUOTE_H' and sn.newvalue = 'T' then sn.date else null end) as date_e,

            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '020%' or sn.newvalue like '02 %') then sn.date else null end) as date_status_20,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '030%' or sn.newvalue like '03 %') then sn.date else null end) as date_status_30,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '040%' or sn.newvalue like '04 %') then sn.date else null end) as date_status_40,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '050%' or sn.newvalue like '05 %') then sn.date else null end) as date_status_50,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '060%' or sn.newvalue like '06 %') then sn.date else null end) as date_status_60,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '065%' or sn.newvalue like '06b %') then sn.date else null end) as date_status_65,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '070%' or sn.newvalue like '07 %') then sn.date else null end) as date_status_70,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '080%' or sn.newvalue like '08 %') then sn.date else null end) as date_status_80,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '090%' or sn.newvalue like '09 %') then sn.date else null end) as date_status_90,
            min(case when sn.field = 'TRANDOC.KENTITYSTATUS' and (sn.newvalue like '100%' or sn.newvalue like '10 %') then sn.date else null end) as date_status_100
        from SystemNote as sn
        join transaction as t on sn.recordid = t.id and t.entitystatus in (8,9,10,20,21,23,30,11,12,14,29)
        where sn.recordtypeid = -30 
                        and sn.field in ('CUSTBODY_QUOTE_A','CUSTBODY_QUOTE_B','CUSTBODY_QUOTE_C','CUSTBODY_QUOTE_D','CUSTBODY_QUOTE_H', 'TRANDOC.KENTITYSTATUS')
        group by sn.recordid
    `;

    var opportunityQuery = `        
        select 
            t.id,
            t.tranid,
            t.entity as CustomerId,
            BUILTIN.DF(t.entity) as Customer,
            --t.subsidiary,
            t.title,
            BUILTIN.DF(t.entitystatus) as CurrentStatus,
            BUILTIN.DF(t.employee) as SalesRep,
            BUILTIN.DF(t.custbody_market_channel) as MarketChannel,
            BUILTIN.DF(t.custbody_product_category) as ProductCategory,
            
            t.createddate as CreatedDate,
            COALESCE(sn2.date, t.createddate) as QuotningDate,
            sn3.date as CustomerReviewDate,
            
            sna.date as SubStatusADate, 
            snb.date as SubStatusBDate,
            snc.date as SubStatusCDate,
            snd.date as SubStatusDDate,
            sne.date as SubStatusEDate,

        from transaction as t
        left join SystemNote as sn2 on sn2.recordtypeid = -30 and sn2.recordid = t.id and sn2.field = 'TRANDOC.KENTITYSTATUS' and sn2.newvalue like '02 %'
        left join SystemNote as sn3 on sn3.recordtypeid = -30 and sn3.recordid = t.id and sn3.field = 'TRANDOC.KENTITYSTATUS' and sn3.newvalue like '03 %'

        left join SystemNote as snA on snA.recordtypeid = -30 and snA.recordid = t.id and snA.field = 'CUSTBODY_QUOTE_A' and snA.newvalue = 'T'
        left join SystemNote as snB on snB.recordtypeid = -30 and snB.recordid = t.id and snB.field = 'CUSTBODY_QUOTE_B' and snB.newvalue like 'T'
        left join SystemNote as snC on snC.recordtypeid = -30 and snC.recordid = t.id and snC.field = 'CUSTBODY_QUOTE_C' and snC.newvalue like 'T'
        left join SystemNote as snD on snD.recordtypeid = -30 and snD.recordid = t.id and snD.field = 'CUSTBODY_QUOTE_D' and snD.newvalue like 'T'
        left join SystemNote as snE on snE.recordtypeid = -30 and snE.recordid = t.id and snE.field = 'CUSTBODY_QUOTE_H' and snE.newvalue like 'T'

        where t.type = 'Opprtnty'
    `;

    function getRequest ( request )
    {
        try
        {
            var queryMap = {
                'bench': benchSamplesQuery,
                'oppdates': opportunityDatesQuery,
                'test': testQuery,
                'opp': opportunityQuery
            };

            if (!queryMap.hasOwnProperty(request.query)) {
                throw new Error('Query Not Found');
            }

            var myQuery = queryMap[request.query]; 
            
            var pageIndex = parseInt(request.pageIndex, 10) || 0;
            var pageSize = parseInt(request.pageSize, 10) || 1000;
            if (pageSize > 5000) { pageSize = 5000; }

            var includeResults = typeof request.includeResults === 'undefined' ? true : 
                            (request.includeResults === 'true' || request.includeResults === true);
        
            var pagedResults = query.runSuiteQLPaged({
                query: myQuery,
                pageSize: pageSize
            });
            
            var queryResults = includeResults ? pagedResults.fetch(pageIndex).data.asMappedResults() : []

            var result = {
                pageIndex: pageIndex,
                pageSize: pageSize,
                pageCount: pagedResults.pageRanges.length,
                totalCount: pagedResults.count,
                resultsCount: queryResults.length,           
                results: queryResults
            };
            return JSON.stringify(result);

        }
        catch(e)
        {
            log.error({
                title: 'Error Caught',
                details: e.message
            });

            return JSON.stringify(e);
        }
        
    }

    return { get: getRequest }
});
