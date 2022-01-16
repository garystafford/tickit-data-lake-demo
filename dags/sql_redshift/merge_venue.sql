BEGIN TRANSACTION;

DELETE
FROM tickit_demo.venue
    USING tickit_demo.venue_staging
WHERE venue.venueid = venue_staging.venueid;

INSERT INTO tickit_demo.venue
SELECT
    venueid,
    venuename,
    venuecity,
    venuestate,
    venueseats
FROM tickit_demo.venue_staging;

END TRANSACTION;
