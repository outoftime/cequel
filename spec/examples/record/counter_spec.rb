# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Counter do
  model :Post do
    key :id, :uuid, auto: true
    column :title, :text
    counter :pageviews
  end

  let! :post do
    Post.create! do |post|
      post.title = 'Cequel'
    end
  end

  describe 'new record' do
    it 'should default counter attribute to zero' do
      post.pageviews.should be_zero
    end
  end

  describe 'updating a counter' do
    before do
      post.pageviews += 4
      post.save!
    end

    it 'should increment counter locally' do
      post.pageviews.should == 4
    end

    it 'should increment counter in the database' do
      cequel[:post_counts].where(id: post.id).first[:pageviews].should == 4
    end
  end
end
