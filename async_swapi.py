'''
https://swapi.py4e.com - ссылка на апи, с которым общаемся
async def - синтаксис описания асинхронной ф-ции
await - должно быть внутри асинхр.ф-ции

corotina - преподготавливаются, разом запускаются, результаты берутся по мере появления
task - задача - начинает выполняться в момент создания, ее окончания не ждут (create_task)
'''
import asyncio                  # библиотека для запуска АФ
import aiohttp                  # асинхронный аналог request
import asyncpg                  # альтернатива psycopg2
import sqlalchemy

from more_itertools import chunked  # позволяет отправлять по 10 запросов к серверу

from models import init_db, People, Session

CHUNK_SIZE = 10                 # ограничиваем кол-во запросов к серверу

async def paste_to_db(people):
    async with Session() as session:
        # people = [People(json=person) for person in people] 
        result = []
        for person in people:
            if 'name' in person:
                # result += [People(json=person, name=person['name'])] #person['birth_year']
                # result += [People(name=person['name'])] #person['birth_year']

                result += [People(birth_year=person['birth_year'],
                                  eye_color=person['eye_color'],
                                  films=str(person['films']),
                #                 #   gender=person['gender'],
                #                 #   hair_color=person['hair_color'],
                #                 #   height=person['height'],
                #                 #   homeworld=person['homeworld'],
                #                 #   mass=person['mass'],
                #                 #   name=person['name'],
                #                 #   skin_color=person['skin_color'],
                #                 #   species=person['species'],
                #                 #   starships=person['starships'],
                #                 #   vehicles=person['vehicles']
                                  )]

        session.add_all(result)
        await session.commit()


async def get_person(person_id, session):         # АФ
    # session = aiohttp.ClientSession()  
    response = await session.get(f'https://swapi.py4e.com/api/people/{person_id}')
    json = await response.json()
    # print(json['birth_year'])
    # birth_year = await response.json['birth_year']
    # print(birth_year)
    # await session.close()
    # return birth_year
    return json

async def get_film(films_list, session):         # АФ
    for film in films_list:
        response = await session.get(f'{film}')
    json = await response.json()
    return json


    
async def main():               # АФ для запуска заавейченной корутины.  В ней можно 
                                # запустить прараллельно неск-ко корутин.

    # session = aiohttp.ClientSession()      # создаем сессию

    await init_db()             # инициализируем базу

    async with aiohttp.ClientSession() as session: # создаем сессию через контектстный мен.

        # coro_1 = get_person(1, session)
        # coro_2 = get_person(2, session)
        # coro_3 = get_person(3, session)
        # coro_4 = get_person(4, session) - вместо перечисления ниже делаем цикл
        
        ## gather_coro = asyncio.gather(coro_1, coro_2, coro_3, coro_4) # собираем корутины
        ## result = await gather_coro  # авейтим gather
        #result = await asyncio.gather(coro_1, coro_2, coro_3, coro_4) # д.б. внутри сессии!!
        
        # await session.close() - не нужно, так как сессия создана через КМ

        for people_id_chunk in chunked(range (1, 100), CHUNK_SIZE): # используем chunked
                                                                    # для запросов по 10
            # coros = []
            # for people_id in people_id_chunk:
            #     coros.append(get_person(people_id, session))
            # меняем на строку ниже:
            coros = [get_person(people_id, session) for people_id in people_id_chunk]
            
            result = await asyncio.gather(*coros)
            
            # await paste_to_db(result)
            asyncio.create_task(paste_to_db(result)) # создаем ТАСК
    
    ## await paste_to_db_task # ждем, пока таски не закончат выполняться

    # asyncio.all_tasks()    # возвращает все висящие на исполнении таски

    tasks_to_await = asyncio.all_tasks() - {asyncio.current_task()} #ожидаем окончания всех
                                            # тасков, кроме самого, в котором ожидаем
    await asyncio.gather(*tasks_to_await)




asyncio.run(main())             #запуск "аф для запуска заавейченной корутины"