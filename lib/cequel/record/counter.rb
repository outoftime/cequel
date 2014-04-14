# -*- encoding : utf-8 -*-
require 'cequel/record/attribute_proxy'

module Cequel
  module Record
    class Counter < DelegateClass(Integer)
      include AttributeProxy
      extend Forwardable

      def_delegators :@model, :loaded?, :incrementer

      def initialize(model, column)
        @model, @column = model, column
      end

      def +(value)
        Delta.new(self, value)
      end

      def update(delta)
        unless delta.is_a?(Delta)
          raise ArgumentError, "Counter columns cannot be set " \
                               "directly. Use += and -= to increment " \
                               "or decrement."
        end
        to_modify do
          @model.__send__(:write_attribute, column_name, __getobj__ + delta.value)
        end
        incrementer.increment(@column.name => delta.value)
      end

      protected

      attr_accessor :delta

      def __getobj__
        @model.__send__(:read_attribute, @column.name)
      end

      def __setobj__(obj)
        fail "Attempted to call __setobj__ on read-only delegate!"
      end

      class Delta < DelegateClass(Integer)
        attr_reader :value

        def initialize(counter, value)
          @counter, @value = counter, value
        end

        protected

        def __getobj__
          @counter + @value
        end

        def __setobj__(obj)
          fail "Attempted to call __setobj__ on read-only delegate!"
        end
      end
    end
  end
end
