from .translate import create_or_update_translation

def create_record(
    models, db, uid, password,
    model, external_id, values,
    translation_field= None, 
    en=None, fr=None
):
    # create record 
    record_id = models.execute(
        db, uid, password,
        model, 'create', values
    )

    # create external id 
    models.execute(
        db, uid, password,
        'ir.model.data', 
        'create',
        {
            'name': external_id,
            'module': '__import__',
            'model': model,
            'res_id': record_id
        }
    )

    if(translation_field is not None):  
        if(fr is not None):
            create_or_update_translation(
                models, db, uid, password,
                translation_field, record_id,
                en, fr, 'fr_CA'
            )
    
    return record_id


def update_record(
    models, db, uid, password,
    model, record_id, values,
    translation_field = None,
    en=None, fr=None 
):
    models.execute_kw(
        db, uid, password,
        model, 'write',
        [record_id, values]
    )

    if(translation_field is not None):
        if (fr is not None):
            create_or_update_translation(
                models, db, uid, password,
                translation_field, record_id, en, fr,
                'fr_CA'
            )
   

