
import logging;logging.basicConfig(level=logging.INFO)
import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

async def creat_pool(loop,**kw):
    logging.info('创建数据库连接池')
    global __pool #创建全局变量
    __pool=await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['root'],
        password=kw['xz3210xz'],
        db=kw['db'],
        charset=kw.get('charset','utf8mb4'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )
async def close_pool():
    '''异步关闭连接池'''
    logging.info('close database connection pool...')
    global __pool
    __pool.close()
    await __pool.wait_closed()

async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (await __pool) as conn:
        cur=await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs=await cur.fetchmany(size)
        else:
            rs=await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql,args):
    log(sql)
    with(await __pool) as conn:
        try:
            cur=await conn.cursor()
            await cur.execute(sql.replace('?','%s'),args)
            affected=cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name=='Model':
            # 排除Model类本身
            return type.__new__(cls, name, bases, attrs)
        # 获取表名
        tableName=attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        mappings=dict()

        primaryKey=None
        for key, val in attrs.copy().items():
            if isinstance(val, Field):
                mappings[key] = attrs.pop(key)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')

        escaped_fields=[]
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = escaped_fields + [primaryKey]  # 全部属性名，主键一定在是最后
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select * from `%s`' % (tableName)
        attrs['__insert__'] = 'insert into `%s` (%s) values (%s)' % (
        tableName, ', '.join('`%s`' % f for f in attrs['__fields__']), ', '.join('?' * len(mappings)))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
        tableName, ', '.join('`%s`=?' % f for f in escaped_fields), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

class Model(dict,metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % item)

    def __setattr__(self, key, value):
        self[key]=value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            field=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self,key,value)
        return value

    @classmethod
    async def find(cls,pk):
        ' find object by primary key. '
        rs=await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs)==0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        rows = await execute(self.__insert__, args)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        rows = await execute(self.__update__, args)

    async def remove(self):
        args = list(map(self.getValue, self.__fields__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)



class Field(object):
    def __init__(self,name,column_type, primary_key, default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)
class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

