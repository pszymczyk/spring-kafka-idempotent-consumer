create table public.events_count(
  events_count_value int ,
  PRIMARY KEY ( events_count_value )
);

create table public.processed_events(
  event_id varchar(100),
  PRIMARY KEY ( event_id )
);

INSERT INTO public.events_count (events_count_value) VALUES ('0');