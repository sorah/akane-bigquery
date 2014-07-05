module AkaneBigquery
  module Schema

    #STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP or RECORD
    #NULLABLE, REQUIRED and REPEATED.
    SCHEMAS = {
      '0' => {
        'tweets' => {
          'fields' => [
            {'name' => 'json', 'type' => 'STRING', 'mode' => 'REQUIRED'},

            {'name' => 'id_str', 'type' => 'STRING', 'mode' => 'REQUIRED'},
            {'name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'},

            {'name' => 'text', 'type' => 'STRING', 'mode' => 'REQUIRED'},

            {'name' => 'lang', 'type' => 'STRING'},
            {'name' => 'source', 'type' => 'STRING'},

            {'name' => 'in_reply_to_status_id_str', 'type' => 'STRING'},
            {'name' => 'in_reply_to_status_id', 'type' => 'INTEGER'},
            {'name' => 'in_reply_to_user_id_str', 'type' => 'STRING'},
            {'name' => 'in_reply_to_user_id', 'type' => 'INTEGER'},
            {'name' => 'in_reply_to_screen_name', 'type' => 'STRING'},

            {'name' => 'created_at', 'type' => 'TIMESTAMP', 'mode' => 'REQUIRED'},

            {
              'name' => 'user', 'type' => 'RECORD', 'mode' => 'REQUIRED',
              'fields' => [
                {'name' => 'id_str', 'type' => 'STRING', 'mode' => 'REQUIRED'},
                {'name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'},
                {'name' => 'name', 'type' => 'STRING', 'mode' => 'REQUIRED'},
                {'name' => 'screen_name', 'type' => 'STRING', 'mode' => 'REQUIRED'},
                {'name' => 'protected', 'type' => 'BOOLEAN', 'mode' => 'NULLABLE'},
              ],
            },

            {'name' => 'coordinates_longitude', 'type' => 'FLOAT'},
            {'name' => 'coordinates_latitude', 'type' => 'FLOAT'},

            {
              'name' => 'place', 'type' => 'RECORD',
              'fields' => [
                {'name' => 'id', 'type' => 'STRING'},
                {'name' => 'country', 'type' => 'STRING'},
                {'name' => 'country_code', 'type' => 'STRING'},
                {'name' => 'name', 'type' => 'STRING'},
                {'name' => 'full_name', 'type' => 'STRING'},
                {'name' => 'place_type', 'type' => 'STRING'},
                {'name' => 'url', 'type' => 'STRING'},
              ],
            },
          ],
        },
        'deletions' => {
          'fields' => [
            {'name' => 'user_id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'},
            {'name' => 'tweet_id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'},
            {'name' => 'user_id_str', 'type' => 'STRING', 'mode' => 'REQUIRED'},
            {'name' => 'tweet_id_str', 'type' => 'STRING', 'mode' => 'REQUIRED'},

            {'name' => 'deleted_at', 'type' => 'TIMESTAMP', 'mode' => 'REQUIRED'},
          ],
        },
        'events' => {
          'fields' => [
            {'name' => 'json', 'type' => 'STRING', 'mode' => 'REQUIRED'},

            {'name' => 'event', 'type' => 'STRING', 'mode' => 'REQUIRED'},

            {'name' => 'source_id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'},
            {'name' => 'target_id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'},
            {'name' => 'source_id_str', 'type' => 'STRING', 'mode' => 'REQUIRED'},
            {'name' => 'target_id_str', 'type' => 'STRING', 'mode' => 'REQUIRED'},

            {'name' => 'target_object_id', 'type' => 'INTEGER'},
            {'name' => 'target_object_id_str', 'type' => 'STRING'},

            {'name' => 'created_at', 'type' => 'TIMESTAMP', 'mode' => 'REQUIRED'},
          ],
        },
      }.freeze,
    }.freeze

    VERSION = '0'
    SCHEMA = SCHEMAS[VERSION]

  end
end
