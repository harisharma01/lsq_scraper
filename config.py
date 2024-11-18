from clientsitter import DataManager
import asyncio
import portalocker
import time

async def main():
    obj_dm = DataManager()
    await obj_dm.fetching_keys()
    obj_dm.fetching_colleges()
    await obj_dm.link_redirect_looping()

if __name__ == "__main__":
    lockfile = 'potallocker.lock'
    
    with open(lockfile, 'w') as f:
        try:
            portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.LockException:
            print('Another instance of this script is already running')
            exit()

        try:
            start_time = time.time()
            asyncio.run(main())
            print(time.time()- start_time)
        finally:
            # Release the lock
            portalocker.unlock(f)