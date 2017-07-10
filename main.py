import os
import asyncpg
import uvloop
import asyncio
import hashlib
from aiohttp import web
from aiohttp.web_exceptions import HTTPNotFound
from PIL import Image

# Настройки
DSN = 'postgres://user:pass@localhost:5432/db_name'
LISTEN_HOST = '127.0.0.1'
LISTEN_PORT = 8083
THUMBNAIL_ROOT = 'thumbs'
ORIGINS_ROOT = 'origins'
WATERMARKS_PATH = ''
QUALITY = 75


def calc_dark_side(pixel):
    if (0.299 * pixel[0] + 0.587 * pixel[1] + 0.114 * pixel[2]) > 127:
        return False
    return True


def add_watermark(image, watermark):
    image = image.convert('RGBA')
    half_origin_width = image.size[0] / 2
    half_origin_height = image.size[1] / 2
    half_the_width = watermark.size[0] / 2
    half_the_height = watermark.size[1] / 2
    box = (
        half_the_width - half_origin_width,
        half_the_height - half_origin_height,
        half_the_width + half_origin_width,
        half_the_height + half_origin_height
    )
    watermark = watermark.convert('RGBA').crop(box)
    rez = Image.composite(watermark, image, watermark, )
    watermark.close()
    return rez


def go_watermark(origin_image, x, y):
    light = 0
    dark = 0
    from_x, _ = divmod(x, 6)
    from_y, _ = divmod(y, 6)
    to_x, _ = divmod(x, 3)
    to_y, _ = divmod(y, 3)
    for iy in range(from_y, to_y):
        for ix in range(from_x, to_x):
            if calc_dark_side(origin_image.getpixel((ix, iy))):
                dark += 1
            else:
                light += 1
    suffix = 'dark'
    if dark > light:
        suffix = 'light'
    try:
        watermark = Image.open(os.path.join(WATERMARKS_PATH, 'watermark_{}.png'.format(suffix)))
    except FileNotFoundError:
        print('NOT found watermark image "watermark_{}.png"'.format(suffix))
        print('RETURNED ORIGINAL IMAGE!!!')
    else:
        return add_watermark(origin_image, watermark)
    return origin_image


def resize_and_crop(img_origin, size, crop_type='middle'):
    img_ratio = img_origin.size[0] / float(img_origin.size[1])
    ratio = size[0] / float(size[1])
    if ratio > img_ratio:
        img_origin = img_origin.resize((size[0], int(round(size[0] * img_origin.size[1] / img_origin.size[0]))),
                                       Image.LANCZOS)
        if crop_type == 'top':
            box = (0, 0, img_origin.size[0], size[1])
        elif crop_type == 'middle':
            box = (0, int(round((img_origin.size[1] - size[1]) / 2)), img_origin.size[0],
                   int(round((img_origin.size[1] + size[1]) / 2)))
        elif crop_type == 'bottom':
            box = (0, img_origin.size[1] - size[1], img_origin.size[0], img_origin.size[1])
        else:
            raise ValueError('ERROR: invalid value for crop_type')
        img_origin = img_origin.crop(box)
    elif ratio < img_ratio:
        img_origin = img_origin.resize((int(round(size[1] * img_origin.size[0] / img_origin.size[1])), size[1]),
                                       Image.LANCZOS)
        if crop_type == 'top':
            box = (0, 0, size[0], img_origin.size[1])
        elif crop_type == 'middle':
            box = (int(round((img_origin.size[0] - size[0]) / 2)), 0,
                   int(round((img_origin.size[0] + size[0]) / 2)), img_origin.size[1])
        elif crop_type == 'bottom':
            box = (img_origin.size[0] - size[0], 0, img_origin.size[0], img_origin.size[1])
        else:
            raise ValueError('ERROR: invalid value for crop_type')
        img_origin = img_origin.crop(box)
    else:
        img_origin = img_origin.resize((size[0], size[1]), Image.LANCZOS)
    return img_origin


async def generate_thumbnail(db_pool, dir_path, file_path, client, resolution, sort_num, product_id):
    try:
        sort_num = int(sort_num)
        product_id = int(product_id)
    except:
        raise HTTPNotFound(text='1')

    image = None
    query = 'SELECT image FROM images_products WHERE client=$1 AND sort=$2 AND product_id=$3 LIMIT 1'
    async with db_pool.acquire() as connection:
        async with connection.transaction():
            db_data = await connection.fetch(query, client, sort_num, product_id)
            if db_data:
                image = os.path.join(ORIGINS_ROOT, db_data[0]['image'])
    if image:
        try:
            os.makedirs(dir_path)
        except:
            pass
        try:
            x, y = resolution.lower().split('x')
            x, y = int(x), int(y)
        except:
            raise HTTPNotFound()

        if 3 > x or 3 > y:
            raise HTTPNotFound()

        origin_image = Image.open(image)
        img_thumb = resize_and_crop(origin_image, (x, y))
        # img_thumb = go_watermark(img_thumb, x, y)
        img_thumb.convert('RGB').save(file_path, 'JPEG', quality=QUALITY)
        img_thumb.close()
        with open(file_path, "rb") as f:  # пробуем открыть тумбнейл с диска
            return web.Response(body=f.read(), content_type="image/jpeg")

    # если надо заглушку на отсутствующее фото то тут
    raise HTTPNotFound()  # если в базе нет то 404


async def handle(request):
    match_info = request.match_info
    client = match_info.get('client', None)
    resolution = match_info.get('resolution', None)
    sort_num = match_info.get('sort_num', None)
    product_id = match_info.get('product_id', None)
    hashed = hashlib.md5("{}-{}-{}-{}".format(client, resolution, sort_num, product_id).encode('utf-8')).hexdigest()
    dir_path = os.path.join(THUMBNAIL_ROOT, hashed[:2], hashed[-2:])
    file_path = os.path.join(dir_path, '{}.jpg'.format(hashed))
    try:
        with open(file_path, "rb") as f:  # пробуем открыть тумбнейл с диска
            return web.Response(body=f.read(), content_type="image/jpeg")
    except IOError:
        return await generate_thumbnail(request.app['pool'], dir_path, file_path, client, resolution, sort_num,
                                        product_id)
    except:
        raise HTTPNotFound(text='3')


async def init_app():
    web_app = web.Application()
    web_app['pool'] = await asyncpg.create_pool(dsn=DSN)
    web_app.router.add_get('/{client}/{resolution}/{sort_num}/{product_id}.jpg', handle)
    return web_app


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    web.run_app(app, host=LISTEN_HOST, port=LISTEN_PORT)
