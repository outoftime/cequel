require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Nesting do
  model :Blog do
    key :subdomain, :text
    column :name, :text
    has_many :posts
  end

  model :Post do
    belongs_to :blog
    key :id, :uuid
    column :title, :text
  end

  describe '::belongs_to' do
    it 'should define partition key' do
      cequel.schema.read_table(:posts).partition_keys.map(&:name).
        should == [:blog_subdomain]
    end

    it 'should define key as clustering column' do
      cequel.schema.read_table(:posts).clustering_columns.map(&:name).
        should == [:id]
    end

    it 'should read from attribute' do
      blog = Blog.new
      blog.subdomain = 'bigdata'
      blog.name = 'Big Data'
      blog.save
      post = Post.new
      post.blog_subdomain = 'bigdata'
      post.blog.should == blog
    end

    it 'should be nil if column is nil' do
      Post.new.blog.should be_nil
    end

    it 'should write to attribute'
  end
end
