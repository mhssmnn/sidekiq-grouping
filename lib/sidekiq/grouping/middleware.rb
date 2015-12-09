module Sidekiq
  module Grouping
    class Middleware
      def call(worker_class, msg, queue, redis_pool = nil)
        worker_class = worker_class.classify.constantize if worker_class.is_a?(String)
        options = worker_class.get_sidekiq_options

        batch =
          options.keys.include?('batch_flush_size') ||
          options.keys.include?('batch_flush_interval')

        passthrough =
          msg['args'] &&
          msg['args'].is_a?(Array) &&
          msg['args'].try(:first) == true

        batch_key = options['batch_key'] || nil

        retrying = msg["failed_at"].present?

        return yield unless batch

        if !(passthrough || retrying)
          add_to_batch(worker_class, queue, batch_key, msg, redis_pool)
        else
          msg['args'].shift if passthrough
          yield
        end
      end

      private

      def add_to_batch(worker_class, queue, batch_key, msg, redis_pool = nil)
        if batch_key.respond_to? :call
          key = batch_key.call( *msg['args'] )
        end

        Sidekiq::Grouping::Batch
          .new(worker_class.name, queue, key, redis_pool)
          .add(msg['args'])

        nil
      end
    end
  end
end
