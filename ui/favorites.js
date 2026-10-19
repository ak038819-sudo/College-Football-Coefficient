/* Favorite teams, kept on the device and nowhere else.

   There is no account and no server here, deliberately: a list of team slugs
   in localStorage needs no login, collects nothing personal, and is the same
   shape a server would store later, so moving it behind a device token does
   not change what the page reads.

   Every entry is a team slug (the `slug` on DATA.teams, which is also what
   #team= carries). Storing slugs rather than ids or names means a saved
   favorite survives a rebuild and resolves through the page's existing
   TEAM_BY_SLUG lookup. Anything that is not a slug is dropped on read, so a
   corrupted or hand-edited value degrades to "no favorites" rather than
   throwing on a page that has nothing to do with favorites.

   Storage can be absent or throw outright -- Safari private browsing, a
   blocked third-party context, a page opened from file:// -- so every access
   is guarded and failure always reads as an empty list. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CfbFavorites = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const KEY = 'elocast.favorites.v1';
  // A ceiling rather than a feature: it bounds what a page has to render and
  // what a sync would later have to carry. Nobody follows two dozen teams.
  const MAX = 24;
  const SLUG = /^[a-z0-9][a-z0-9-]{0,59}$/;

  function store(storage) {
    if (storage) return storage;
    try {
      return (typeof localStorage !== 'undefined' && localStorage) || null;
    } catch (err) {
      return null;                      // reading the property itself can throw
    }
  }

  function valid(slug) {
    return typeof slug === 'string' && SLUG.test(slug);
  }

  /** The saved slugs, in the order they were added. Never throws. */
  function read(storage) {
    const target = store(storage);
    if (!target) return [];
    let raw;
    try {
      raw = target.getItem(KEY);
    } catch (err) {
      return [];
    }
    if (!raw) return [];
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      return [];
    }
    if (!Array.isArray(parsed)) return [];
    const seen = new Set();
    const out = [];
    for (const slug of parsed) {
      if (!valid(slug) || seen.has(slug)) continue;
      seen.add(slug);
      out.push(slug);
      if (out.length === MAX) break;
    }
    return out;
  }

  /** Writes a cleaned list and returns what was written. Never throws. */
  function write(storage, slugs) {
    const target = store(storage);
    const seen = new Set();
    const clean = [];
    for (const slug of Array.isArray(slugs) ? slugs : []) {
      if (!valid(slug) || seen.has(slug)) continue;
      seen.add(slug);
      clean.push(slug);
      if (clean.length === MAX) break;
    }
    if (!target) return clean;
    try {
      target.setItem(KEY, JSON.stringify(clean));
    } catch (err) {
      // A full or read-only store. The caller still gets the list it asked
      // for, so the page it just drew is consistent with what the user did;
      // it simply will not be there next visit.
    }
    return clean;
  }

  function has(storage, slug) {
    return read(storage).indexOf(slug) !== -1;
  }

  function add(storage, slug) {
    if (!valid(slug)) return read(storage);
    const current = read(storage);
    if (current.indexOf(slug) !== -1) return current;
    return write(storage, current.concat([slug]));
  }

  function remove(storage, slug) {
    const current = read(storage);
    if (current.indexOf(slug) === -1) return current;
    return write(storage, current.filter(s => s !== slug));
  }

  /** Adds or removes, and returns the list as it now stands. */
  function toggle(storage, slug) {
    return has(storage, slug) ? remove(storage, slug) : add(storage, slug);
  }

  return { KEY, MAX, read, write, has, add, remove, toggle };
});
