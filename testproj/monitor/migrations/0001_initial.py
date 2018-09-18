# Generated by Django 2.1 on 2018-09-11 05:01

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ContentItem',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=256, verbose_name='Название')),
                ('content', models.TextField(verbose_name='Контент')),
                ('weight', models.IntegerField(default=1, verbose_name='Вес')),
            ],
            options={
                'verbose_name': 'блок контента',
                'verbose_name_plural': 'блоки контента',
                'ordering': ['-weight'],
            },
        ),
        migrations.CreateModel(
            name='Page',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=256, verbose_name='Название')),
                ('link', models.CharField(max_length=256, verbose_name='Ссылка')),
                ('contents', models.ManyToManyField(blank=True, to='monitor.ContentItem')),
            ],
            options={
                'verbose_name': 'страница',
                'verbose_name_plural': 'страницы',
            },
        ),
    ]
