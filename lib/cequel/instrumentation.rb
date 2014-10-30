module Cequel
  module Instrumentation
    module ModuleMethods
      # Instruments `method_name` to publish the value returned by the
      # `data_builder` proc onto `topic`
      #
      # Example:
      #
      #    extend Instrumentation
      #    instrument :create, "create.cequel", data: {table_name: table_name}
      #
      # @param method_name [Symbol,String] The method to instrument
      #
      # @param topic [String] The name with which to publish this
      #   instrumentation
      #
      # @option opts [Object] :data_method (nil) the data to publish along
      #   with the notification. If it responds to `#call` it will be
      #   called with the record object and the return value used for
      #   each notification.
      def instrument(method_name, topic, opts)
        data = opts[:data]

        data_proc = if data.respond_to? :call
                      data
                    else
                      ->(_){ data }
                    end

        define_method(:"__data_for_#{method_name}_instrumentation", &data_proc)

        module_eval <<-METH
          def #{method_name}_with_instrumentation(*args)
            instrument("#{topic.to_s}", __data_for_#{method_name}_instrumentation(self)) do
              #{method_name}_without_instrumentation(*args)
            end
          end
        METH

        alias_method_chain method_name, "instrumentation"
      end
    end

    protected

    def instrument(name, data, &blk)
      ActiveSupport::Notifications.instrument(name, data, &blk)
    end

    # Module Methods

    def self.included(a_module)
      a_module.extend ModuleMethods
    end
  end
end
