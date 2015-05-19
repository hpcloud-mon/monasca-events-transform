# monasca-events-transform

To run transform engine:

python monasca_events_transform/main.py

To generate events:

python monasca_events_transform/generator.py

To add transform definitions:

python monasca_events_transform/definition monasca_events_transform/event_definitions.yaml add A

To remove transform definitions:

python monasca_events_transform/definition monasca_events_transform/event_definitions.yaml delete A
