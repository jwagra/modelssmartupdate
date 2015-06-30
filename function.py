# -*- coding: utf-8 -*-
def get_chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

# херачит апдэйт моделей одним запросом => pg
def models_smart_update(models, **kwargs):
    from django.db import connection
    from django.contrib.gis.db.models import Model
    import datetime
    from mx.DateTime import DateTimeDeltaType
    from core.fields import format_mxtimedelta
    pk_name = kwargs.get('pk_name', 'id')
    chunks = kwargs.get('chunks', 1000)

    def get_prepared_value(value):
        if type(value) in (datetime.timedelta, DateTimeDeltaType):
            value = "'%s'" % format_mxtimedelta(value)
        elif value is None:
            value = "NULL"
        elif isinstance(value, Model):
            value = "'%s'" % value.id
        else:
            value = "'%s'" % value
        return value

    def shock_db(models_list, table_name, columns, cf_dict, dt_dict):
        all_values = []
        for model in models_list:
            values = ["%s" % getattr(model, cf_dict[pk_name].name)]
            for column in columns:
                field = cf_dict[column]
                try: value = getattr(model, field.attname)
                except: value = None
                if type(value) is list:
                    value = "'{%s}'" % ','.join('"%s"' % v for v in value)
                else:
                    value = get_prepared_value(value)
                db_type = dt_dict[column]
                if 'geometry' in db_type:
                    values.append('ST_SetSRID(%s, 4326)' % (value +'::'+ 'geometry',))
                else:
                    values.append(value +'::'+ db_type)
            all_values.append('('+ (','.join('%s' % v for v in values)) +')')
        all_columns = [pk_name]; all_columns.extend(columns)
        query = '''
            UPDATE %(table_name)s AS t
            SET %(columns_set)s
            FROM (VALUES
                %(all_values)s
            ) AS c (%(all_columns)s)
            WHERE c.%(pk_name)s = t.%(pk_name)s
        ''' % {
            'table_name': table_name, 'pk_name': pk_name,
            'columns_set': ','.join('"%(c)s" = c.%(c)s' % { 'c': c } for c in columns),
            'all_columns': ','.join('"%s"' % c for c in all_columns),
            'all_values': ','.join('%s' % v for v in all_values)
        }
        cursor = connection.cursor()
        cursor.execute(query)

    if models:
        models_list = list(models)
        first_model = models_list[0]
        table_name = first_model._meta.db_table
        columns = []; cf_dict = {}; dt_dict = {}
        for f in first_model._meta.fields:
            if f.column != pk_name:
                columns.append(f.column)
            cf_dict[f.column] = f
            dt_dict[f.column] = f.db_type(connection)
        if chunks:
            chunks_list = list(get_chunks(models_list, chunks))
            for c in chunks_list:
                shock_db(c, table_name, columns, cf_dict, dt_dict)
        else:
            shock_db(models_list, table_name, columns, cf_dict, dt_dict)
        return True
    return False
