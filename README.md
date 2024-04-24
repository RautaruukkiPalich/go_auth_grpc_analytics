реализация аналитики клиента для https://github.com/RautaruukkiPalich/go_auth_grpc

Можно стартануть в 3 этапа:
1) Переименовать существующий конфиг файл из local_sample.yaml в sample.yaml
2) Настроить в нём данные для kafka (или запустить докер из https://github.com/RautaruukkiPalich/go_auth_grpc )
3) Выполнить команду 
```sh 
make run
```