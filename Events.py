
from neo4j import GraphDatabase
import random
from datetime import datetime
from uuid import uuid4

#////////////////Connecting ////////////
URI = "bolt://your_neo4j_server_address:7687"
AUTH = ("your_neo4j_username", "your_neo4j_password")
employee_threshold = 10

##################### DATA ###############################

Person_info = [
    {"name": "Geralt", "surname": "of Rivia", "age": 80},
    {"name": "Yennefer", "surname": "of Vengerberg", "age": 99},
    {"name": "Ciri", "surname": "of Cintra", "age": 22},
    {"name": "Triss", "surname": "Merigold", "age": 38},
    {"name": "Jaskier", "surname": "the Bard", "age": 35},
    {"name": "Dandelion", "surname": "the Bard", "age": 40},
    {"name": "Vesemir", "surname": "dsf", "age": 120},
    {"name": "Eskel", "surname": "sdf", "age": 105},
    {"name": "Lambert", "surname": "dsf", "age": 100},
    {"name": "Shani", "surname": "dsf", "age": 40},
]

Attended_event = [
    {"name": "Geralt", "event": "Comic Con"},
    {"name": "Yennefer", "event": "Vyno dienos"},
    {"name": "Ciri", "event": "Vyno dienos"},
    {"name": "Triss", "event": "Vilniaus knygų mugė"},
    {"name": "Geralt", "event": "Vyno dienos"},
    {"name": "Yennefer", "event": "Vilniaus knygų mugė"},
    {"name": "Ciri", "event": "Vilniaus knygų mugė"},
    {"name": "Triss", "event": "Vyno dienos"},
    {"name": "Jaskier", "event": "Comic Con"},
    {"name": "Dandelion", "event": "Vilniaus knygų mugė"},
    {"name": "Vesemir", "event": "Vilniaus knygų mugė"},
    {"name": "Eskel", "event": "Comic Con"},
    {"name": "Lambert", "event": "Vilniaus knygų mugė"},
    {"name": "Shani", "event": "Comic Con"},
]

Hobby = [
    {"name": "Cosplay"},
    {"name": "Komiksai"},
    {"name": "Fantastika"},
    {"name": "Serialai"},
    {"name": "Kolekcionavimas"},
    {"name": "Vynas"}, 
    {"name": "Knygos"},
    {"name": "Literatura"},
    {"name": "Rasytojai"},
    {"name": "Degustacija"},
]

Person_Hobbies = [
    {"name": "Geralt", "hobby": "Cosplay"},
    {"name": "Yennefer", "hobby": "Fantastika"},
    {"name": "Ciri", "hobby": "Cosplay"},
    {"name": "Triss", "hobby": "Fantastika"},
    {"name": "Jaskier", "hobby": "Knygos"},
    {"name": "Dandelion", "hobby": "Komiksai"},
    {"name": "Vesemir", "hobby": "Vynas"},
    {"name": "Eskel", "hobby": "Literatura"},
    {"name": "Lambert", "hobby": "Rasytojai"},
    {"name": "Shani", "hobby": "Degustacija"},
    {"name": "Geralt", "hobby": "Komiksai"},
    {"name": "Yennefer", "hobby": "Vynas"},
    {"name": "Ciri", "hobby": "Knygos"},
    {"name": "Triss", "hobby": "Vynas"},
    {"name": "Jaskier", "hobby": "Serialai"},
    {"name": "Dandelion", "hobby": "Kolekcionavimas"},
    {"name": "Eskel", "hobby": "Degustacija"},
    {"name": "Shani", "hobby": "Literatura"},
    {"name": "Geralt", "hobby": "Degustacija"},
    {"name": "Yennefer", "hobby": "Kolekcionavimas"},
    {"name": "Triss", "hobby": "Rasytojai"},
]

Event_categoty = [
    {"name": "Comic Con", "hobby": "Cosplay"},
    {"name": "Comic Con", "hobby": "Fantastika"},
    {"name": "Comic Con", "hobby": "Komiksai"},
    {"name": "Comic Con", "hobby": "Serialai"},
    {"name": "Comic Con", "hobby": "Kolekcionavimas"},
    {"name": "Comic Con", "hobby": "Knygos"},
    {"name": "Vyno dienos", "hobby": "Kolekcionavimas"},
    {"name": "Vyno dienos", "hobby": "Vynas"},
    {"name": "Vyno dienos", "hobby": "Degustacija"},
    {"name": "Vilniaus knygų mugė", "hobby": "Fantastika"},
    {"name": "Vilniaus knygų mugė", "hobby": "Serialai"},
    {"name": "Vilniaus knygų mugė", "hobby": "Kolekcionavimas"},
    {"name": "Vilniaus knygų mugė", "hobby": "Rasytojai"},
    {"name": "Vilniaus knygų mugė", "hobby": "Knygos"},
    {"name": "Vilniaus knygų mugė", "hobby": "Literatura"},
]

Similar_hobbies = [
    {"hobby1": "Kolekcionavimas", "hobby2": "Vynas"},
    {"hobby1": "Kolekcionavimas", "hobby2": "Komiksai"},
    {"hobby1": "Degustacija", "hobby2": "Vynas"},
    {"hobby1": "Literatura", "hobby2": "Knygos"},
    {"hobby1": "Knygos", "hobby2": "Fantastika"},
    {"hobby1": "Knygos", "hobby2": "Komiksai"},
    {"hobby1": "Fantastika", "hobby2": "Komiskai"},
    {"hobby1": "Komiksai", "hobby2": "Cosplay"},
    {"hobby1": "Rasytojai", "hobby2": "Knygos"},
]

##############################################################################

def person(tx, character):
    # Create new Person node with given character details
    result = tx.run("""
        MERGE (p:Person {name: $name, surname: $surname, age: $age})
        RETURN p.name AS name, p.surname AS surname, p.age AS age
        """, name=character["name"], surname=character["surname"], age=character["age"]
    )

    return result.single()

def create_event_tx(tx, event_name):
    result = tx.run("""
        CREATE (e:Event {name: $event_name, date: datetime()})
        RETURN e.name AS name
        """, event_name=event_name
    )
    return result.single()["name"]


def create_Hobby(tx, Hobby_name):
    result = tx.run("""
        CREATE (h:Hobby {name: $Hobby_name, date: datetime()})
        RETURN h.name AS name
        """, Hobby_name=Hobby_name
    )
    return result.single()["name"]

############################### Relationships ##############################

def create_custom_relationships(tx, person_name, event_name):
    result = tx.run("""
        MATCH (p:Person {name: $person_name})
        MATCH (e:Event {name: $event_name})
        MERGE (p)-[:ATTENDED]->(e)
        RETURN p.name AS person_name, e.name AS event_name
        """, person_name=person_name, event_name=event_name
    )
    return result.single()

def create_hobby_relationships(tx, person_name, hobby_name):
    result = tx.run("""
        MATCH (p:Person {name: $person_name})
        MATCH (h:Hobby {name: $hobby_name})
        MERGE (p)-[:HAS_HOBBY]->(h)
        RETURN p.name AS person_name, h.name AS hobby_name
        """, person_name=person_name, hobby_name=hobby_name
    )
    return result.single()

def create_Event_category_relationships(tx, event_name, hobby_name):
    result = tx.run("""
        MATCH (e:Event {name: $event_name})
        MATCH (h:Hobby {name: $hobby_name})
        MERGE (h)-[:IS_CATEGORY]->(e)
        RETURN e.name AS event_name, h.name AS hobby_name
        """, event_name=event_name, hobby_name=hobby_name
    )
    return result.single()

def create_similar_hobbies_relationship(tx, hobby1_name, hobby2_name):
    result = tx.run("""
        MERGE (hobby1:Hobby {name: $hobby1_name})
        MERGE (hobby2:Hobby {name: $hobby2_name})
        MERGE (hobby1)-[:SIMILAR_TO]->(hobby2)
        MERGE (hobby2)-[:SIMILAR_TO]->(hobby1)
        RETURN hobby1.name AS hobby1_name, hobby2.name AS hobby2_name
        """, hobby1_name=hobby1_name, hobby2_name=hobby2_name)

    return result.single()

##############################################################################

def delete_all_data(tx):
    tx.run("MATCH (n) DETACH DELETE n")

################ Queries ################

def get_attendees_of_event_tx(tx, event_name):
    result = tx.run("""
        MATCH (p:Person)-[:ATTENDED]->(e:Event {name: $event_name})
        RETURN p.name AS person_name
        """, event_name=event_name)

    return [record["person_name"] for record in result]

def find_person_by_surname(tx, surname):
    result = tx.run("""
        MATCH (p:Person {surname: $surname})
        RETURN p.name AS name, p.surname AS surname, p.age AS age
        """, surname=surname)

    return [record["name"] for record in result]

def find_all_events_attended_by_person(tx, person_name):
    result = tx.run("""
        MATCH (p:Person {name: $person_name})-[:ATTENDED]->(e:Event)
        RETURN COLLECT(DISTINCT e.name) AS events
        """, person_name=person_name)

    return result.single()["events"]

def count_people_attending_event(tx, event_name):
    result = tx.run(
        """
        MATCH (p:Person)-[:ATTENDED]->(e:Event {name: $event_name})
        RETURN COUNT(DISTINCT p) AS attendee_count
        """,
        event_name=event_name,
    )

    record = result.single()
    if record:
        return record["attendee_count"]
    else:
        return 0 

def find_shortest_path_between_entities_by_hobbies(tx, start_entity_name, end_entity_name):
    result = tx.run(
        """
        MATCH (start {name: $start_entity_name}),
              (end {name: $end_entity_name}),
              path = shortestPath((start)-[*]-(end))
        WHERE length(path) > 0
        RETURN [node in nodes(path) | node.name] AS related_entities
        """,
        start_entity_name=start_entity_name,
        end_entity_name=end_entity_name,
    )

    record = result.single()
    if record:
        return record["related_entities"]
    else:
        return None

def find_paths_between_entities_by_hobbies(tx, start_entity_name, end_entity_name, max_path_length=None):
    if max_path_length is not None:
        max_path_length_condition = f"{max_path_length}"
    else:
        max_path_length_condition = ""

    result = tx.run(
        """
        MATCH path = (start:Event {name: $start_entity_name})-[*1..""" + max_path_length_condition + """]-(end:Event {name: $end_entity_name})
        RETURN nodes(path) AS nodes
        """,
        start_entity_name=start_entity_name,
        end_entity_name=end_entity_name,
    )

    records = result.data()
    unique_paths = set()

    for record in records:
        path_nodes = record["nodes"]

        formatted_path = []

        for i in range(len(path_nodes)):
            formatted_path.append(f"{path_nodes[i]['name']}")

        unique_paths.add(tuple(formatted_path))

    return list(unique_paths)

##################################################################################


def main():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:

            #session.write_transaction(delete_all_data)
            #print("All data deleted from the database.")
            
            ########### Creating data and relationships #########

            for character in Person_info:
                person_result = session.write_transaction(person, character)
                #print(f"Character {person_result['name']} {person_result['surname']} added with age {person_result['age']}")


            for i in range(3):
                event_name = ["Comic Con", "Vyno dienos", "Vilniaus knygų mugė"][i]
                event_name_created = session.write_transaction(create_event_tx, event_name)
                #print(f"Event {event_name_created} created.")


            for hob in Hobby:
                Hobby_created = session.write_transaction(create_Hobby, hob['name'])
                #print(f"Hobby {Hobby_created} created.")

            for link in Attended_event:
                link_result = session.write_transaction(create_custom_relationships, link['name'], link['event'])
                #print(f"Character {link_result['person_name']} attended {link_result['event_name']}.")

            for link in Person_Hobbies:
                hobby_result = session.write_transaction(create_hobby_relationships, link['name'], link['hobby'])
                #print(f"Character {hobby_result['person_name']} has a hobby: {hobby_result['hobby_name']}.")
            
            for link in Event_categoty:
                event_category_result = session.write_transaction(create_Event_category_relationships, link['name'], link['hobby'])
                #print(f"Event {event_category_result['event_name']} has a category: {event_category_result['hobby_name']}.")
                
            for link in Similar_hobbies:
                relationship_result = session.write_transaction(create_similar_hobbies_relationship, link['hobby1'], link['hobby2'])
                #print(f"Hobby '{relationship_result['hobby1_name']}' is similar to hobby '{relationship_result['hobby2_name']}'.")
            
    
            
            while True:
                print("Choose an option:")
                print("1. People who attended a specific event [Comic Con]")
                print("2. People with a specific surname [Merigold]")
                print("3. Events attended by a specific person [Geralt]")
                print("4. Amount of people who attended specific event [Comic Con]")
                print("5. Shortest path between your two choices [Geralt, Dandelion]")
                print("6. Find all paths between events [Comic Con, Vyno dienos]")
                print("7. Exit")

                choice = input("Enter the number of your choice: ")

                if choice == "1":
                    event_name = input("Enter the event name: ")
                    attendees  = session.write_transaction(get_attendees_of_event_tx, event_name)
                    print(f"People who attended '{event_name}': {attendees}")
                elif choice == "2":
                    surname = input("Enter the surname: ")
                    found_people = session.write_transaction(find_person_by_surname, surname)
                    print(f"People with surname '{surname}': {found_people}")
                elif choice == "3":
                    person_name = input("Enter the person name: ")
                    events_attended = session.write_transaction(find_all_events_attended_by_person, person_name)
                    print(f"Events attended by {person_name}: {events_attended}")
                elif choice == "4":
                    event_name = input("Enter the event name: ")
                    attendee_count = session.read_transaction(count_people_attending_event, event_name)
                    print(f"{attendee_count} people attented '{event_name}'.")
                elif choice == "5":
                    person_name = input("Enter choice number one: ")
                    event_name = input("Enter choice number two: ")
                    linked_entities = session.write_transaction(find_shortest_path_between_entities_by_hobbies, person_name, event_name)
                    print(linked_entities)
                elif choice == "6":
                    event1_name = input("Enter the first event name: ")
                    event2_name = input("Enter the second event name: ")
                    max_path_length = int(input("Enter the maximum path length (or press Enter for all paths): ") or "0")
                    paths = find_paths_between_entities_by_hobbies(session, event1_name, event2_name, max_path_length)

                    print(f"All paths between {event1_name} and {event2_name}:")
                    for i, path in enumerate(paths, 1):
                        print(f"{i}. {path}")
                elif choice == "7":
                    print("Exiting...")
                    break
                else:
                    print("Invalid choice. Please enter a number between 1 and 6.")

            

if __name__ == "__main__":
    main()
