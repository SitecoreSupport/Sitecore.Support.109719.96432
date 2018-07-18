namespace Sitecore.Support.ContentSearch.LuceneProvider
{
  using System.Linq;
  using System.Threading;
  using Lucene.Net.Index;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.LuceneProvider;
  using Sitecore.ContentSearch.LuceneProvider.Sharding;
  using Sitecore.ContentSearch.Maintenance;
  using Sitecore.ContentSearch.Sharding;

  public class LuceneIndex : Sitecore.ContentSearch.LuceneProvider.LuceneIndex
  {
    protected override void PerformRefresh(IIndexable indexableStartingPoint, IndexingOptions indexingOptions, CancellationToken cancellationToken)
    {
      this.VerifyNotDisposed();

      if (!this.ShouldStartIndexing(indexingOptions))
        return;

      lock (this.indexUpdateLock)
      {
        if (!this.Crawlers.Any(c => c.HasItemsToIndex()))
          return;

        using (var context = this.CreateUpdateContext())
        {
          foreach (var crawler in this.Crawlers)
          {
            crawler.RefreshFromRoot(context, indexableStartingPoint, indexingOptions, cancellationToken);
          }

          context.Commit();
        }
      }
    }

    public LuceneIndex(string name, string folder, IIndexPropertyStore propertyStore, string @group) : base(name, folder, propertyStore, @group)
    {
    }

    public LuceneIndex(string name, string folder, IIndexPropertyStore propertyStore) : base(name, folder, propertyStore)
    {
    }

    public LuceneIndex(string name) : base(name)
    {
    }


    protected void InitializeWithCustomScheduler(IndexWriter writer, int threadsLimit = -1)
    {
      ConcurrentMergeScheduler mergeScheduler = new Sitecore.Support.ConcurrentMergeScheduler();
      if (threadsLimit != -1)
      {
        mergeScheduler.MaxThreadCount = threadsLimit;
      }
      writer.SetMergeScheduler(mergeScheduler);
    }

    protected override void DoReset(IProviderUpdateContext context)
    {
      base.VerifyNotDisposed();

      foreach (LuceneShard shard in this.Shards)
      {
        lock (this)
        {
          SupportResetWithNewScheduler(shard, this);
        }
      }
    }

    void SupportResetWithNewScheduler(LuceneShard shard, LuceneIndex index)
    {
      lock (shard)
      {
        using (IndexWriter writer = new IndexWriter(shard.Directory, ((LuceneIndexConfiguration)index.Configuration).Analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
        {
          writer.DeleteAll();
          writer.Commit();
        }
        using (IndexWriter writer = shard.CreateWriter(new LuceneUpdateContext(index), LuceneIndexMode.CreateNew))
        {
          this.InitializeWithCustomScheduler(writer, -1);
        }
      }
    }

    public override IndexWriter CreateWriter(IProviderUpdateContext context, Shard shard, LuceneIndexMode mode)
    {
      this.EnsureInitialized();
      Shard tmpshard = this.Shards.First<Shard>(x => x.Id == shard.Id);
      if (tmpshard is LuceneShard)
      {
        return ShardHelper.CreateWriterWithNewScheduler(context, tmpshard as LuceneShard, this, mode);
      }
      return null;
    }
  }
}