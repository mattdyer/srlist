import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  port="3307",
  user="root",
  password="",
  database="anonymized"
)

mycursor = mydb.cursor()

#sql = "SELECT logID, BID FROM mrlogs WHERE bid = %s"

sql = """SELECT
          l.logID AS logID,
          l.DateAdded AS DateAdded,
          trim(l.Status) AS Status,
          l.Rating,
          l.BillableRequestStatus,
          v.Name AS ServiceTypeName,
          v.Name AS ServiceType,
          Trim(l.Location) AS Location,
          l.Company AS CompanyName,
          l.Company AS Company,
          CASE WHEN MAX(lu.date) is not null 
          then 
            CASE WHEN MAX(lu.date) > l.dateupdated 
            then 
              MAX(lu.Date) 
            else 
              l.dateupdated 
            end 
          else
            l.dateupdated
          end as DateUpdated,
          b.Name AS BuildingName,
          b.Name AS Building,
          trim(l.ContactPerson) AS ContactPerson,
          l.CompanyID AS CompanyID,
          mt.BID AS TenantCID,
          l.ServiceID AS ServiceID,
          COALESCE(l.Priority, 3) AS Priority,
          COALESCE(mat.Priority, 3) AS DesigneePriority,
          l.initialized,
          mrrl.recurringMode,
          group_concat(DISTINCT mrad.requestDate) AS additionalRequestDates,
          group_concat(DISTINCT mrid.ignoreDate) AS ignoreDates,
          b.BID AS BID,
          l.billableTemplateID,
          l.BuildingID AS BuildingID,
            l.UserID AS UserID,
            l.Email AS Email,
            l.SuiteFloor AS SuiteFloor,
            l.Telephone AS Telephone,
            l.Fax AS Fax,
            l.OtherDesc AS OtherDesc,
            l.Contractor AS Contractor,
            l.CtrCompany AS CtrCompany,
            l.CtrEmail AS CtrEmail,
            l.CtrTelephone AS CtrTelephone,
            l.CtrContact AS CtrContact,
            trim(l.RequestDesc) AS RequestDesc,
            l.EmailList AS EmailList,
            l.Custom1 AS Custom1,
            l.Custom2 AS Custom2,
            l.Custom3 AS Custom3,
            l.Custom4 AS Custom4,
            l.Custom5 AS Custom5,
            l.Billable AS Billable,
            l.BillableStatus AS BillableStatus,
            l.TotalTime AS TotalTime,
            l.RepeatRequest,
            l.Archived,
          l.mrrecurringlogid,
            v.id AS ServiceID,
            CONCAT(mt.FirstName,' ',mt.LastName) AS TenantName,
          mb.EnableSatisfactionSurvey,
          holds.id AS HoldID,
          mrrl.frequencyType,
          l.scheduled,
          group_concat(DISTINCT trim(a.Name) ORDER BY trim(a.Name) SEPARATOR ', ') AS DesigneeName,
          group_concat(DISTINCT trim(a.Name) ORDER BY trim(a.Name) SEPARATOR ', ') AS Designee,
          group_concat(DISTINCT a.id) AS DesigneeIDList,
          group_concat(DISTINCT bl.id) AS BillableItemIDList
        FROM
            mrlogs l
        LEFT OUTER JOIN 
          mrassigneetasks mat
        ON 
          mat.MRLOGID = l.logID AND (mat.Status != 'deleted' or mat.Status is null)
        LEFT OUTER JOIN 
          mrassignee a
        ON
          a.id = mat.AID AND a.live = 'yes'
        LEFT OUTER JOIN
            mrserviceslinked v
        ON
          l.ServiceID = v.id
        LEFT OUTER JOIN
          masterbuildinglist b
        ON
          l.BID = b.BID
        LEFT OUTER JOIN
          mastertenants mt
        ON
          mt.id = l.UserID
        LEFT OUTER JOIN 
          mrlogupdates lu 
        ON 
          lu.lid = l.LogID
          and lu.logType != 'communication'

        LEFT OUTER JOIN
          mrlogbillable bl
        ON
          bl.lid = l.logid AND bl.status != 'deleted'

        LEFT OUTER JOIN
          mrbuildings mb
        ON
          mb.BID = l.BID

        LEFT OUTER JOIN
          mrholds holds
        ON
          holds.logid = l.logid
          and holds.live = 1
          and holds.type = 'manual'
          and
            (
              holds.startdate <= DATE_SUB(now(), INTERVAL -ifnull(b.timezone,0) HOUR) and 
              (
                holds.enddate >= DATE_SUB(now(), INTERVAL -ifnull(b.timezone,0) HOUR)
                OR holds.enddate is null
              )
            )
        
        LEFT OUTER JOIN
          mrrecurringlogs mrrl
        ON
          l.mrrecurringlogid = mrrl.id
        LEFT OUTER JOIN
          mrrecurringlogadditionaldates mrad
        ON
          mrad.mrrecurringlogid = mrrl.id and mrad.live = 1
        LEFT OUTER JOIN
          mrrecurringlogignoredates mrid
        ON
          mrid.mrrecurringlogid = mrrl.id and mrid.live = 1
        WHERE
          b.bookStatus != 'off'
          AND (
            l.Status != 'deleted'
            or l.Status is null
          )
          AND l.bid = %s
          AND l.initialized = 1
        GROUP BY 
          l.logID"""


requests = []

fetched_results = {}


for x in range(500):
  for next_bid in [1023, 1473]:
    
    if not next_bid in fetched_results:

      bid = (next_bid, )

      mycursor.execute(sql, bid)

      myresult = mycursor.fetchall()

      #print(myresult)

      fields = [i[0] for i in mycursor.description]
      result = [dict(zip(fields,row))   for row in myresult]
      
      fetched_results[next_bid] = result
      
    else:
      result = fetched_results[next_bid]


    requests = requests + result


requests = sorted(requests, key = lambda i: i['DateAdded'])  

print(len(requests))  
print(requests[0])
print(requests[1])
print(requests[10])