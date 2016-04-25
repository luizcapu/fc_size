# Setup

Need to set the following env vars:

CONNECTION DO ADTS DB
------------------------

DB_ADTS_JDBC_URL

DB_ADTS_PASS

DB_ADTS_USER


CONNECTION DO CONTROL TABLE DB
--------------------------------

DB_JDBC_URL

DB_PASS

DB_USER


CREATE TABLE IN CONTROL TABLE DB
--------------------------------

```
DROP TABLE IF EXISTS `st_control`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `st_control` (
  `st_id` int(11) NOT NULL,
  `pending` int(11) DEFAULT '1',
  PRIMARY KEY (`st_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
```
