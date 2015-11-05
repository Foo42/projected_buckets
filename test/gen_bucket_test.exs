defmodule ProjectedBuckets.GenBucketTests  do
    use ExUnit.Case
    alias ProjectedBuckets.GenBucket

    test "bucket process starts with start_link" do
      assert {:ok, _pid} = GenBucket.start_link
    end

    test "values can be put and retrieved" do
      {:ok, bucket} = GenBucket.start_link
      GenBucket.put(bucket, "foo", 42)
      assert GenBucket.get(bucket, "foo") == 42
    end

    test "Can create with a mapping function to map key / values when they are inserted" do
      double_values = fn {key,value} -> {key,value * 2} end
      {:ok, bucket} = GenBucket.start_link double_values

      GenBucket.put(bucket, "foo", 42)
      assert GenBucket.get(bucket, "foo") == 84
    end

    test "stream_changes returns stream of bucket changes" do
      test_pid = self()
      {:ok, bucket} = GenBucket.start_link
      spawn_link fn ->
        bucket |> GenBucket.stream_changes |> Stream.each(fn change -> send(test_pid, change) end) |> Stream.run
      end

      :timer.sleep(50) #Got to be a better way to know that the stream iterator is in place
      GenBucket.put(bucket, "foo", 42)
      assert_receive {:put, {"foo", 42}}
    end

    test "can install a view with a mapping function and query mapped values using view name" do
      {:ok, bucket} = GenBucket.start_link
      GenBucket.put(bucket, "old", 1)
      double_values = fn {key,value} -> {key,value * 2} end
      GenBucket.install_view(bucket, "doubled", double_values)
      :timer.sleep(50)
      GenBucket.put(bucket, "foo", 42)
      :timer.sleep(50)
      assert GenBucket.get(bucket, "doubled", "foo") == 84, "values inserted after view should be mapped"
      assert GenBucket.get(bucket, "doubled", "old") == 2, "original values should be mapped" #Tricky to do without there being a gap between getting full content and starting update stream
    end


    test "can install a another bucket process as a view" do
      {:ok, master} = GenBucket.start_link

      double_values = fn {key,value} -> {key,value * 2} end
      {:ok, view_bucket} = GenBucket.start_link double_values

      GenBucket.put(master, "old", 1)

      GenBucket.install_view(master, "doubled", view_bucket)
      :timer.sleep(50)

      GenBucket.put(master, "foo", 42)
      :timer.sleep(50)
      assert GenBucket.get(view_bucket, "foo") == 84, "values inserted after view should be mapped, and accessable in view bucket"
      assert GenBucket.get(view_bucket, "old") == 2, "original values should be mapped and accessable in view bucket" #Tricky to do without there being a gap between getting full content and starting update stream
      assert GenBucket.get(master, "doubled", "foo") === 84, "values in view can be accessed via original bucket using view name"
    end
end
