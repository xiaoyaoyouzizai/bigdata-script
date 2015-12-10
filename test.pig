REGISTER /home/cloudera/pigprofit.jar
A = LOAD '/user/cloudera/orders.hgame.data.csv' USING PigStorage(',') AS (entry:chararray,
lotteryid:chararray,
methodid:chararray,
packageid:chararray,
taskid:chararray,
projectid:chararray,
fromuserid:chararray,
touserid:chararray,
agentid:chararray,
adminid:chararray,
adminname:chararray,
ordertypeid:chararray,
title:chararray,
amount:chararray,
description:chararray,
prebalance:chararray,
prehold:chararray,
preavailable:chararray,
channelbalance:chararray,
holdbalance:chararray,
availablebalance:chararray,
clientip:chararray,
proxyip:chararray,
times:chararray,
date:chararray,
actiontime:chararray,
channelid:chararray,
transferuserid:chararray,
transferchannelid:chararray,
transferorderid:chararray,
transferstatus:chararray,
uniquekey:chararray,
modes:chararray);


B = FOREACH A GENERATE SUBSTRING(times,1,14) as time, SUBSTRING(lotteryid,1,LAST_INDEX_OF(lotteryid,'\'')) as game, com.lc.backoffice.pig.Profit(SUBSTRING(ordertypeid,1,LAST_INDEX_OF(ordertypeid,'\'')),(float)SUBSTRING(amount,1,LAST_INDEX_OF(amount,'\''))) as income;


-- DUMP B;
C = GROUP B BY (time,game);
-- DUMP C;
D = FOREACH C GENERATE FLATTEN(group), SUM(B.income), 'A';
STORE D INTO '/user/cloudera/neworders.csv';