# -*- encoding : utf-8 -*-
module Cequel
  module Record
    module AttributeProxy
      extend Forwardable

      #
      # @!method column_name
      #   @return [Symbol] the name of the proxied column
      #
      def_delegator :@column, :name, :column_name

      def_delegators :__getobj__, :clone, :dup

      #
      # @!method loaded?
      #   @return [Boolean] `true` if the attribute is loaded into memory
      #
      def_delegators :@model, :loaded?

      #
      # @param model [Record] record that contains this attribute
      # @param column [Schema::Column] column this attribute belongs in
      # @return [Collection] a new proxied attribute
      #
      def initialize(model, column)
        @model, @column = model, column
      end

      #
      # @return [String] inspected underlying Ruby object
      #
      def inspect
        __getobj__.inspect
      end

      #
      # Notify the proxy that its underlying data is loaded in memory.
      #
      # @return [void]
      #
      # @api private
      #
      def loaded!
        modifications.each { |modification| modification.call() }.clear
      end

      #
      # Notify the proxy that its staged changes have been written to the
      # data store.
      #
      # @return [void]
      #
      # @api private
      #
      def persisted!
        modifications.clear
      end

      protected

      def __getobj__
        model.__send__(:read_attribute, column_name)
      end

      def __setobj__(obj)
        fail "Attempted to call __setobj__ on read-only delegate!"
      end

      private

      attr_reader :model, :column

      def to_modify(&block)
        if loaded?
          model.__send__("#{column_name}_will_change!")
          block.call()
        else
          modifications << block
        end
        self
      end

      def modifications
        @modifications ||= []
      end
    end
  end
end
