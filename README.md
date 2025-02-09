# ВКР "Исследование протоколов распределённого консенсуса"

- ./pre-defence-1 - ресурсы для первой предзащиты
- ./consensus - кодовая база для ВКР (key-value распределенная система)
- ./consensus/kv-store-client - HTTP клиент для взаимодействия с ratis kv store по GRPC API 
- ./consensus/kv-store-ratis - Непосредственная имплементация kv store на основе библиотеки Apache Ratis и mapDB.
- ./consensus/kv-store-ratis-api - API сообщений, которые могут передавать друг другу kv-store-client и kv-store-ratis
