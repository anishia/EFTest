using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using EFCoreTest.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace EFCoreTest.Services;

public class CodingTestService(AppDbContext db, ILogger<CodingTestService> logger) : ICodingTestService
{
    private readonly AppDbContext _db = db;
    private readonly ILogger<CodingTestService> _logger = logger;

    public async Task GeneratePostSummaryReportAsync(int maxItems)
    {
        // Task placeholder:
        // - Emit REPORT_START, then up to `maxItems` lines prefixed with "POST_SUMMARY|" and
        //   finally REPORT_END. Each summary line must include PostId|AuthorName|CommentCount|LatestCommentAuthor.
        // - Method must be read-only and efficient for large datasets;
        // Implement the method body in the assessment; do not change the signature.
        try{
        
        var postInfo = (from p in _db.Post.AsNoTracking()
                join a in _db.User.AsNoTracking() on p.AuthorId equals a.Id
               
                join c in _db.Comment.AsNoTracking() on p.Id equals c.PostId into postComments
                from pc in postComments.DefaultIfEmpty()
                
                join ca in _db.User.AsNoTracking() on pc.AuthorId equals ca.Id into commentAuthors
                from ca in commentAuthors.DefaultIfEmpty() 
                select new {
                    PostId = p.Id,
                    PostAuthor = a.AuthorName,
                    CommentAuthor = ca != null ? ca.AuthorName : null, // Handle null authors
                    CreatedAt = (DateTime?)pc.CreatedAt // Nullable for posts with no comments
                });
        var postSummary = (from p in postInfo
                           group p by new in {p.PostId,p.PostAuthor} into postSum
                           select new {
                            PostId = postSum.Key.PostId,
                            AuthorName = postSum.Key.PostAuthor,
                            CommentCount = postSum.Key.Count, 
                            LatestCommentAuthor = postSum
                                    .OrderByDescending(x => x.CreatedAt)
                                    .Select(x => x.CommentAuthor)
                                    .FirstOrDefault() 
                           }),Take(maxItems);
        _logger.LogInformation("REPORT_START");       
        var data = await postSummary.AsAsyncEnumerable();

        await Parallel.ForEachAsync(data, async (p, ct) => 
        {
            _logger.LogInformation("POST_SUMMARY|{0}|{1}|{2}|{3}", 
                p.PostId, p.AuthorName, p.CommentCount, p.LatestCommentAuthor);
            await Task.CompletedTask;
        });
        _logger.LogInformation("REPORT_END");
        }
        Catch(Exception ex)
        {
            _logger.LogError(ex,"Exception Occured");
            throw;
        }
        //throw new NotImplementedException("Implement GeneratePostSummaryReportAsync according to assessment requirements.");
    }

    public async Task<IList<PostDto>> SearchPostSummariesAsync(string query, int maxResults = 50)
    {
        try{
            if(query)
            {
              var result = await SearchPostSummary(query,0,maxResults);
            }
            else{
             _logger.LogWarning("Enter query");
             throw;
            }
        }
        Catch(Exception ex)
        {
            _logger.LogError(ex,"Exception Occured");
            throw; 
        }
        // Task placeholder:
        // - Return at most `maxResults` PostDto entries.
        // - Treat null/empty/whitespace query as no filter (return unfiltered results up to maxResults).
        // - Matching: case-insensitive substring in Title OR Content.
        // - Order by CreatedAt descending, project to PostDto, and avoid materializing full entities.
        // Implement the method body in the assessment; do not change the signature.

        //throw new NotImplementedException("Implement SearchPostSummariesAsync according to assessment requirements.");
    }

    public async Task<IList<PostDto>> SearchPostSummariesAsync<TKey>(string query, int skip, int take, Expression<Func<PostDto, TKey>> orderBySelector, bool descending)
    {
        try{
            if(query)
            {
              var result = await SearchPostSummary(query,skip,take);
              if (descending)
                return result.OrderByDescending(orderBySelector);
            else
                return source.OrderBy(orderBySelector);
            }
            else{
            _logger.LogWarning("Enter query");
            throw;
            }
        }
        Catch(Exception ex)
        {
            _logger.LogError(ex,"Exception Occured");
            throw; 
        }
        // Task placeholder:
        // - Server-side filter by `query` (null/empty => no filter), server-side ordering based on
        //   the provided DTO selector, then Skip/Take for paging. Project to PostDto and avoid
        //   per-row queries or client-side paging.
        // - Implementations may choose which selectors to support; unsupported selectors may
        //   be rejected by the grader.
        // Implement the method body in the assessment; do not change the signature.
        //throw new NotImplementedException("Implement SearchPostSummariesAsync (paged) according to assessment requirements.");
    }
    private async Task<IList<PostDto>> SearchPostSummary(string query,int skip,int take){
       try{
         var postInfo = await (from  p in  _db.Post.AsNoTracking()
                   join c in _db.Comment.AsNoTracking() on p.Id equals c.PostId into postComment
                   from pc in postComment.DefaultIfEmpty()
                   join a in  _db.User.AsNoTracking() on p.AuthorId equals a.Id
                   select new {
                        p.PostId,
                        p.Title,
                        p.Content,
                        PostAuthor = a.AuthorName,
                        PostAuthorId = a.Id,
                        CommentId = pc.Id,
                        p.CreatedAt
                   }).ToAsyncEnumerable().Skip(skip).Take(take);
        var postSummary = (from p in postInfo
                           group p by in {p.PostId,p.p.Title,p.Content,p.CreatedAt,p.PostAuthor} into postSum
                           select new  PostDto{
                            Id = postSum.Key.PostId,
                            Title= postSum.Key.Title,
                            Excerpt = postSum.Key.Content,
                            AuthorName = postSum.Key.PostAuthor,
                            CommentCount = postSum.Key.Count, 
                            CreatedAt=postSum.Key.CreatedAt;
                           });
        var postsummaryResult = await (from  p in postSummary
                         where p.Title.Contains(query,StringComparison.OrdinalIgnoreCase) || a.Excerpt.Contains(query,StringComparison.OrdinalIgnoreCase))
                         .ToListAsync();
                       
                  
        
       return postsummaryResult;
    }
}
