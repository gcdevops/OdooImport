

def create_or_update_translation(
    models,
    db,
    uid,
    password,
    name,
    row_id,
    src,
    value,
    lang
):
    existing_translation = models.execute_kw(
        db, uid, password,
        'ir.translation', 'search_read',
        [[
            '&', '&', 
            ('name', '=', name),
            ('res_id', '=', row_id),
            ('lang', '=', lang)
        ]],
        {
            'fields': ['id']
        }
    )

    if existing_translation:
        models.execute_kw(
            db,
            uid,
            password,
            'ir.translation',
            'write',
            [
                existing_translation[0]['id'],
                {
                    'src': src,
                    'value': value
                }
            ]
        )
    else:
        models.execute(
            db,
            uid,
            password,
            'ir.translation',
            'create',
            {
                'name': name,
                'res_id': row_id,
                'lang': lang,
                'type': 'model',
                'src': src,
                'value': value,
                'module': '__import__',
                'state': 'translated'
            }
        )
