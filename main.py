import asyncio
import datetime
from platform import system

import aiohttp
from more_itertools import chunked

from models import Session, SwapiPeople, close_orm, init_orm


MAX_COROS = 5 # change to 1 to test like a synchronous code
MAX_PEOPLE = 200


async def get_deep_data(url, key, session):
    response = await session.get(f"{url}")
    json_data = await response.json()
    return json_data[key]


async def parse_and_insert_to_database(list_json: list[dict]):
    async with Session() as db_session:
        async with aiohttp.ClientSession() as get_deep_data_session:
            for item in list_json:
                if item.get('detail', None) == "Not found":
                    continue

                print(f"Processing: {item['name']}")
                if item.get('films', []) != []:
                    coros_films = [get_deep_data(film_url, 'title', get_deep_data_session) for film_url in item['films']]
                    films_str = ','.join(await asyncio.gather(*coros_films))
                else:
                    films_str = None
                if item.get('species', []) != []:
                    coros_species = [get_deep_data(species_url, 'name', get_deep_data_session) for species_url in item['species']]
                    species_str = ','.join(await asyncio.gather(*coros_species))
                else:
                    species_str = None
                if item.get('starships', []) != []:
                    coros_starships = [get_deep_data(starship_url, 'name', get_deep_data_session) for starship_url in item['starships']]
                    starships_str = ','.join(await asyncio.gather(*coros_starships))
                else:
                    starships_str = None
                if item.get('vehicles', []) != []:
                    coros_vehicles = [get_deep_data(vehicle_url, 'name', get_deep_data_session) for vehicle_url in item['vehicles']]
                    vehicles_str = ','.join(await asyncio.gather(*coros_vehicles))
                else:
                    vehicles_str = None
                if item.get('homeworld', None) != None:
                    coros_homeworld = get_deep_data(item['homeworld'], 'name', get_deep_data_session)
                    homeworld_str = await coros_homeworld
                else:
                    homeworld_str = None

                person = SwapiPeople(
                    name=item.get('name', None),
                    birth_year=item.get('birth_year', None),
                    eye_color=item.get('eye_color', None),
                    gender=item.get('gender', None),
                    hair_color=item.get('hair_color', None),
                    height=item.get('height', None),
                    mass=item.get('mass', None),
                    skin_color=item.get('skin_color', None),
                    homeworld=homeworld_str,
                    films=films_str,
                    species=species_str,
                    starships=starships_str,
                    vehicles=vehicles_str,
                )
                db_session.add(person)
                await db_session.commit()
                print(f"Finished: {item['name']}")


async def get_people(people_id, session):
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}/")
    json_data = await response.json()
    return json_data


async def main():
    await init_orm()

    db_tasks = []
    async with aiohttp.ClientSession() as session:
        for coros_chunk in chunked(range(1, MAX_PEOPLE + 1), MAX_COROS):
            coros = [get_people(i, session) for i in coros_chunk]
            people = await asyncio.gather(*coros)
            db_tasks.append(asyncio.create_task(parse_and_insert_to_database(people)))

    for coros_chunk in chunked(range(0, len(db_tasks)), MAX_COROS):
        coros = [db_tasks[i] for i in coros_chunk]
        await asyncio.gather(*coros)

    await close_orm()


start = datetime.datetime.now()
# https://stackoverflow.com/questions/45600579/asyncio-event-loop-is-closed-when-getting-loop
if system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
print(datetime.datetime.now() - start)
