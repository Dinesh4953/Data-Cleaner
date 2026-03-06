from mongoengine import Document, DictField, IntField

class DataRows(Document):
    user_id = IntField(required=True)
    data = DictField()


# from django.db import models

# # Create your models here.
# from django.db import models

# class DataRows(models.Model):
#     data = models.JSONField()

#     class Meta:
#         ordering = ['id']
