BEGIN TRANSACTION;

DELETE
FROM tickit_demo.event
    USING tickit_demo.event_staging
WHERE event.eventid = event_staging.eventid;

INSERT INTO tickit_demo.event
SELECT *
FROM tickit_demo.event_staging;

END TRANSACTION;