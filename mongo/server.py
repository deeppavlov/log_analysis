from fastapi import FastAPI
import asyncio
from uvicorn import Config, Server
from datetime import datetime, timedelta

from plot import plot_pdf
from poller import upd

from fastapi.responses import FileResponse

some_file_path = "report.pdf"
app = FastAPI()


@app.get("/")
async def main():
    return FileResponse(some_file_path)


async def update():
    while True:
        x = datetime.now()
        upd()
        plot_pdf()
        delta = datetime.now()-x
        await asyncio.sleep(24*60*60-delta.seconds)


loop = asyncio.get_event_loop()
loop.create_task(update())
config = Config(app=app, loop=loop, host='0.0.0.0')
server = Server(config)
loop.run_until_complete(server.serve())
