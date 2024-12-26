require "mongoid_retry/version"

module Mongoid
  module MongoidRetry

    DUPLICATE_KEY_ERROR_CODES = [11000,11001]
    MAX_RETRIES = 3

    def self.is_a_duplicate_key_error?(exception)
      DUPLICATE_KEY_ERROR_CODES.include?(exception.code)
    end

    # Catch a duplicate key error
    def save_and_retry(options = {})
      begin
        result = with(write_concern: { w: 1 }).save!
        result
      rescue Mongo::Error::OperationFailure => e
        result = retry_if_duplicate_key_error(e, options)
        result
      end
    end

    def retry_if_duplicate_key_error(e, options)
      retries = options.fetch(:retries, MAX_RETRIES)
      if ::Mongoid::MongoidRetry.is_a_duplicate_key_error?(e) && retries > 0
        keys = duplicate_key(e)
        if (duplicate = find_duplicate(keys))
          if options[:allow_delete]
            duplicate.delete
            save_and_retry(options)
          else
            update_document!(duplicate, options.merge(retries: retries - 1))
            self.attributes = duplicate.attributes.except(:_id)
          end
        end
      else
        raise e
      end
    end

    private

    def find_duplicate(keys)
      self.class.where(keys).first
    end

    # [11000]: E11000 duplicate key error collection: sn_test_master.subseason_player_stats index: stat_module_id_1_subseason_id_1_team_id_1_player_id_1 dup key: { stat_module_id: ObjectId('65e5fb893349e7d9482bc2cf'), subseason_id: 2, team_id: 1, player_id: 5 } (on localhost:27017, legacy retry, attempt 1)
    def duplicate_key(exception)
      str = exception.message[/\{[^{}]+\}/,0]
      if str
        str.gsub!('ObjectId', 'BSON::ObjectId').gsub!('null', 'nil')
        eval(str)
      end
    end

    def update_document!(duplicate, options = {})
      attributes.except("_id").each_pair do |key, value|
        duplicate[key] = value
      end
      duplicate.save_and_retry(options)
    end

  end
end
