using Lucene.Net.Index;
using Lucene.Net.Store;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.LuceneProvider;
using Sitecore.ContentSearch.LuceneProvider.Sharding;
using Sitecore.ContentSearch.Sharding;
using Sitecore.ContentSearch.Utilities;
using Sitecore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Sitecore.Support
{
  public class ShardHelper
  {
    protected readonly LuceneShard luceneShard;
    protected readonly LuceneIndex index;


    public ShardHelper(LuceneShard luceneShard, LuceneIndex index)
    {
      this.luceneShard = luceneShard;
      this.index = index;
    }

     IndexWriter CreateWriter(Directory directory, LuceneIndexMode mode)
    {
      typeof(LuceneShard).GetMethod("EnsureInitialized", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).Invoke(luceneShard, new object[] { });
      Assert.ArgumentNotNull(directory, "directory");
      lock (this)
      {
        bool create = mode == LuceneIndexMode.CreateNew;
        create |= !IndexReader.IndexExists(directory);
        IContentSearchConfigurationSettings instance = this.index.Locator.GetInstance<IContentSearchConfigurationSettings>();
        IndexWriter writer = new IndexWriter(directory, ((LuceneIndexConfiguration)this.index.Configuration).Analyzer, create, IndexWriter.MaxFieldLength.UNLIMITED);
        LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy(writer);
        writer.TermIndexInterval = instance.TermIndexInterval();
        writer.MergeFactor = instance.IndexMergeFactor();
        writer.MaxMergeDocs = instance.MaxMergeDocs();
        writer.UseCompoundFile = instance.UseCompoundFile();
        mp.MaxMergeMB = instance.MaxMergeMB();
        mp.MinMergeMB = instance.MinMergeMB();
        mp.CalibrateSizeByDeletes = instance.CalibrateSizeByDeletes();
        writer.SetMergePolicy(mp);
        writer.SetRAMBufferSizeMB((double)instance.RamBufferSize());
        writer.SetMaxBufferedDocs(instance.MaxDocumentBufferSize());
        ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler
        {
          MaxThreadCount = instance.ConcurrentMergeSchedulerThreads()
        };
        writer.SetMergeScheduler(mergeScheduler);
        typeof(LuceneShard).GetField("lastWriterCreated", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).SetValue(luceneShard, writer);
        return writer;
      }

    }
    internal static IndexWriter CreateWriterWithNewScheduler(IProviderUpdateContext context, LuceneShard shard, LuceneIndex index, LuceneIndexMode mode)
    {
      var shardHelper = new ShardHelper(shard, index);
      return shardHelper.CreateWriter(shard.Directory, mode);
    }
  }
}